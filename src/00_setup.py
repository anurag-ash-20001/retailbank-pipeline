# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates (idempotently) the schemas, volumes, audit/control tables, gold dimension & fact tables and the
# MAGIC data-quality rule catalogue. Safe to run before every batch.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC This is the **first task of the job**. It builds the "empty building" before any data arrives:
# MAGIC * 5 schemas (one per layer) and 2 Volumes (raw landing files + immutable archive)
# MAGIC * audit tables that record what every run did (batches, files, profiles, counts, checks)
# MAGIC * the quarantine table for rejected records
# MAGIC * the gold star schema: 3 dimension tables + the fact table + the fact history table
# MAGIC * a catalogue of every data-quality rule (code → meaning → action)
# MAGIC
# MAGIC Every statement uses `CREATE ... IF NOT EXISTS`, so running it again changes nothing (idempotent).
# MAGIC Defining the gold tables up front (instead of letting Spark infer them) fixes the data types,
# MAGIC adds IDENTITY surrogate keys and documents every table/column with COMMENTs visible in Unity Catalog.
# MAGIC
# MAGIC The next cell loads the shared configuration and helpers (`CATALOG`, `BRONZE`, `SILVER`, `GOLD`, `AUDIT`, …).

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# 1) One schema per medallion layer. The COMMENT is shown in Catalog Explorer, which documents the design.
for schema, comment in [("rb_landing", "Raw landing zone (Volumes) - files exactly as received"),
                        ("rb_bronze", "Bronze - raw records as STRING + ingestion metadata, immutable per batch"),
                        ("rb_silver", "Silver - cleansed, standardized, validated records + quarantine"),
                        ("rb_gold", "Gold - conformed SCD2 dimensions, transaction fact, KPI tables"),
                        ("rb_audit", "Pipeline metadata - batches, ingestion log, profiling, load stats, DQ checks")]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{comment}'")

# 2) Volumes = governed folders for FILES (tables hold rows, Volumes hold files).
#    raw     : where each source drop is uploaded, one sub-folder per batch
#    archive : the pipeline copies every file here unchanged (audit / replay requirement of the brief)
spark.sql(f"CREATE VOLUME IF NOT EXISTS {LANDING}.raw COMMENT 'Landing zone: source drops, one folder per batch'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {LANDING}.archive COMMENT 'Immutable copy of every ingested file, per batch (audit / replay)'")

# COMMAND ----------

# MAGIC %md ## Audit / control tables

# COMMAND ----------

# 3) Pipeline metadata tables (rb_audit) + the quarantine (rejects path).
#    batch_control    : which batches ran, their business date and status (RUNNING -> SUCCEEDED)
#    ingestion_log    : one row per source file: name, size, md5 checksum, row count, structural problems
#    data_profile     : column statistics of the raw data (nulls, distinct values, format patterns)
#    load_stats       : counters per batch/entity (received, passed, rejected, new, changed, corrected ...)
#    dq_check_results : result of every automated quality assertion of every run
#    quarantine       : every rejected record with its reason codes and the untouched raw row
ddl = [
f"""CREATE TABLE IF NOT EXISTS {AUDIT}.batch_control (
  batch_id STRING, batch_seq INT, source_folder STRING, business_date DATE, status STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP, job_run_id STRING)
  COMMENT 'One row per batch/day; status RUNNING -> SUCCEEDED'""",
f"""CREATE TABLE IF NOT EXISTS {AUDIT}.ingestion_log (
  batch_id STRING, batch_seq INT, entity STRING, source_file STRING, landing_path STRING, archive_path STRING,
  file_size_bytes BIGINT, file_md5 STRING, row_count BIGINT, structural_issues STRING, ingested_at TIMESTAMP)
  COMMENT 'Ingestion metadata: file name, row count, ingestion timestamp, batch id'""",
f"""CREATE TABLE IF NOT EXISTS {AUDIT}.data_profile (
  batch_id STRING, entity STRING, column_name STRING, total_rows BIGINT, null_or_blank BIGINT, distinct_values BIGINT,
  has_leading_trailing_space BIGINT, min_length INT, max_length INT, top_values STRING, top_patterns STRING, profiled_at TIMESTAMP)
  COMMENT 'Column profile of every bronze batch (drives the cleansing rules)'""",
f"""CREATE TABLE IF NOT EXISTS {AUDIT}.load_stats (
  batch_id STRING, batch_seq INT, stage STRING, entity STRING, metric STRING, value BIGINT, recorded_at TIMESTAMP)
  COMMENT 'Per-batch counters: received / passed / rejected / new / scd2_changed / corrected ...'""",
f"""CREATE TABLE IF NOT EXISTS {AUDIT}.dq_check_results (
  batch_id STRING, check_name STRING, entity STRING, severity STRING, status STRING,
  observed STRING, expected STRING, checked_at TIMESTAMP)
  COMMENT 'Automated data-quality assertions executed in every run'""",
f"""CREATE TABLE IF NOT EXISTS {SILVER}.quarantine (
  batch_id STRING, batch_seq INT, entity STRING, stage STRING, source_file STRING, source_row_num BIGINT,
  record_key STRING, reason_code STRING, reason_codes ARRAY<STRING>, severity STRING, raw_record STRING,
  quarantined_at TIMESTAMP, resolved_batch_id STRING, resolution STRING)
  COMMENT 'Rejected records with reason codes and untouched raw payload. Nothing is silently dropped.'""",
]
for d in ddl:
    spark.sql(d)

# COMMAND ----------

# MAGIC %md ## Gold model
# MAGIC * `dim_customer`, `dim_branch`, `dim_product` – SCD Type 2, surrogate key = IDENTITY, natural key + `is_current`
# MAGIC * `fact_transaction` – **grain: one row per transaction_id** (latest corrected version); prior versions in `fact_transaction_history`

# COMMAND ----------

# 4) GOLD STAR SCHEMA
# Columns that every SCD Type 2 dimension needs (see scd2_upsert in includes/common):
#   hash_scd2 / hash_type1 : fingerprints used to detect changes cheaply
#   version                : 1, 2, 3 ... per natural key
#   effective_from / _to   : the period in which this version was valid
#   is_current             : TRUE only for the latest version (fast filter for "current state")
#   created/updated_batch  : which batch created / last touched the row (lineage)
SCD_COLS = """hash_scd2 STRING, hash_type1 STRING, version INT, effective_from TIMESTAMP, effective_to TIMESTAMP,
  is_current BOOLEAN, created_batch_id STRING, updated_batch_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP"""

# Dimensions. Two kinds of key:
#   natural key   (customer_id)  = the business identifier from the source system
#   surrogate key (customer_sk)  = a number generated by Delta (IDENTITY), one per VERSION of the customer.
# The fact table stores the surrogate key of the version that was valid when the transaction happened,
# so history is preserved (e.g. a transaction made while KYC was "Pending" stays linked to that version).
gold_ddl = [
f"""CREATE TABLE IF NOT EXISTS {GOLD}.dim_customer (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY COMMENT 'Surrogate key (one per version)',
  customer_id STRING NOT NULL COMMENT 'Natural key',
  customer_name STRING, email STRING, phone STRING, account_id STRING, gender STRING, dob DATE,
  address STRING COMMENT 'SCD2 tracked', city STRING COMMENT 'SCD2 tracked', kyc_status STRING COMMENT 'SCD2 tracked',
  registration_date DATE, ref_transaction_id STRING, potential_duplicate_of STRING, dq_flags ARRAY<STRING>,
  {SCD_COLS}) COMMENT 'Customer dimension (SCD2 on kyc_status, address, city)'""",
f"""CREATE TABLE IF NOT EXISTS {GOLD}.dim_branch (
  branch_sk BIGINT GENERATED ALWAYS AS IDENTITY, branch_id STRING NOT NULL,
  branch_name STRING, location STRING, manager_name STRING COMMENT 'SCD2 tracked', opened_date DATE,
  region STRING COMMENT 'SCD2 tracked', branch_type STRING COMMENT 'SCD2 tracked', contact_number STRING, dq_flags ARRAY<STRING>,
  {SCD_COLS}) COMMENT 'Branch dimension (SCD2 on manager_name, region, branch_type)'""",
f"""CREATE TABLE IF NOT EXISTS {GOLD}.dim_product (
  product_sk BIGINT GENERATED ALWAYS AS IDENTITY, product_id STRING NOT NULL,
  product_name STRING, product_type STRING COMMENT 'SCD2 tracked', price DECIMAL(18,2) COMMENT 'SCD2 tracked',
  launch_date DATE, is_active BOOLEAN COMMENT 'SCD2 tracked', category STRING COMMENT 'SCD2 tracked', vendor_name STRING, dq_flags ARRAY<STRING>,
  {SCD_COLS}) COMMENT 'Product dimension (SCD2 on price, is_active, category, product_type)'""",
]
# Fact table columns. GRAIN = one row per transaction_id (the most recent, corrected version).
#   *_sk               : surrogate keys -> point-in-time links to the dimension versions
#   *_id               : natural keys kept for easy joins / debugging
#   signed_amount      : +amount for normal, -amount for refunds (so SUM() gives the net value)
#   is_inr             : non-INR rows are kept but excluded from INR value KPIs
#   source_date_*      : the original free-text date and how it was parsed (audit of the date fix)
#   record_hash        : fingerprint of the business columns, used to detect late corrections
#   version / is_corrected / correction_count : how many times a late correction changed the row
#   source/first/last_batch_id : lineage - which batch delivered / inserted / last updated the row
FACT_COLS = """transaction_id STRING NOT NULL,
  customer_sk BIGINT, product_sk BIGINT, branch_sk BIGINT,
  account_id STRING, customer_id STRING, product_id STRING, branch_id STRING,
  txn_ts TIMESTAMP, txn_date DATE, txn_hour INT,
  amount DECIMAL(18,2), signed_amount DECIMAL(18,2), currency STRING, is_inr BOOLEAN,
  payment_method STRING, status STRING, is_refund BOOLEAN, remarks STRING,
  source_date_raw STRING, source_date_parsed DATE, dq_flags ARRAY<STRING>,
  record_hash STRING, version INT, is_corrected BOOLEAN, correction_count INT,
  source_batch_id STRING, first_batch_id STRING, last_batch_id STRING, loaded_at TIMESTAMP, updated_at TIMESTAMP"""
# fact_transaction_history has the same columns plus "which batch replaced this version and when".
# When Day 2 corrects T0577 (Failed -> Success), the old version 1 is copied here and the fact row becomes version 2.
gold_ddl += [
f"""CREATE TABLE IF NOT EXISTS {GOLD}.fact_transaction ({FACT_COLS})
  COMMENT 'Transaction fact. Grain = one row per transaction_id (latest version). Late corrections update in place.'""",
f"""CREATE TABLE IF NOT EXISTS {GOLD}.fact_transaction_history ({FACT_COLS},
  superseded_by_batch_id STRING, superseded_at TIMESTAMP)
  COMMENT 'Prior versions of corrected transactions (audit trail of late-arriving corrections)'""",
]
for d in gold_ddl:
    spark.sql(d)

# COMMAND ----------

# MAGIC %md ## Data-quality rule catalogue
# MAGIC Every reason code / flag used by the pipeline, what it means and what the pipeline does with it.

# COMMAND ----------

# 5) DATA-QUALITY RULE CATALOGUE
# Every code that can appear in rb_silver.quarantine.reason_code or in a row's dq_flags array is listed here
# with its meaning and the action taken. Reports join to this table so a reviewer can read
# "ORPHAN_PRODUCT -> product_id not in product dimension -> Quarantine, retried every run".
# rule_type:  REJECT = record goes to quarantine | REJECT(INFO) = duplicate removed (not an error)
#             FLAG   = value repaired or noted, record still loaded | FILE = problem with the whole file
RULES = [
 # entity, code, type, action, description
 ("all", "DUPLICATE_EXACT", "REJECT(INFO)", "Keep first copy, quarantine the rest", "Re-sent record identical after trimming / case normalization"),
 ("dimensions", "DUPLICATE_SUPERSEDED", "REJECT(INFO)", "Keep most complete / latest copy", "Same natural key, different payload (dimension feeds: last-write-wins after completeness)"),
 ("all", "INVALID_KEY", "REJECT", "Quarantine", "Natural key missing or not in expected pattern (C/B/P/T + digits)"),
 ("customers", "MISSING_ACCOUNT_ID", "REJECT", "Quarantine", "Customer has no linked account - cannot join transactions"),
 ("customers", "SHARED_ACCOUNT_ID", "REJECT", "Quarantine non-owner", "Two customers claim one account_id. Owner = customer whose number matches the account number, else existing owner, else earliest registration"),
 ("customers", "EMAIL_REPAIRED", "FLAG", "Fix", "Double '@@' collapsed or missing '@' before domain inserted; lower-cased/trimmed"),
 ("customers", "EMAIL_INVALID", "FLAG", "Set NULL", "Email still invalid after repair"),
 ("customers", "EMAIL_MISSING", "FLAG", "Keep NULL", "Email blank"),
 ("customers/branches", "PHONE_STANDARDIZED", "FLAG", "Fix", "Phone re-formatted to +91-XXXXXXXXXX"),
 ("customers/branches", "PHONE_INVALID", "FLAG", "Set NULL", "Phone does not have 10 digits (after removing +91)"),
 ("customers", "GENDER_STANDARDIZED", "FLAG", "Fix", "M/MALE/f/... mapped to Male/Female/Other/Unknown"),
 ("customers", "KYC_STANDARDIZED", "FLAG", "Fix", "Casing / whitespace in kyc_status normalized"),
 ("customers", "DOB_MISSING", "FLAG", "Keep NULL", "Date of birth blank"),
 ("customers", "DOB_FUTURE", "FLAG", "Set NULL", "Date of birth after the processing date"),
 ("customers", "DOB_IMPLAUSIBLE", "FLAG", "Set NULL", "Age over 100 years"),
 ("all", "DATE_FORMAT_STANDARDIZED", "FLAG", "Fix", "Non ISO date (yyyy/MM/dd, dd-MM-yyyy, dd/MM/yyyy, MM/dd/yyyy) converted"),
 ("all", "DATE_AMBIGUOUS_DDMM", "FLAG", "Assume dd/MM/yyyy", "Both day and month <= 12; Indian convention assumed"),
 ("customers", "REGISTRATION_DATE_FUTURE", "FLAG", "Keep", "Registration after the processing date"),
 ("customers", "POTENTIAL_DUPLICATE_PERSON", "FLAG", "Keep, link", "Different customer_id with same name + date of birth (likely same person, separate account)"),
 ("branches", "MANAGER_MISSING", "FLAG", "Keep NULL", "Branch has no manager name"),
 ("branches", "REGION_STANDARDIZED", "FLAG", "Fix", "Region casing/whitespace normalized"),
 ("branches", "BRANCH_TYPE_STANDARDIZED", "FLAG", "Fix", "Branch type casing normalized"),
 ("products", "JSON_STRUCTURAL_DEFECT", "FILE", "Salvage object-by-object", "File is not valid JSON (e.g. missing comma between objects / truncated / concatenated feed)"),
 ("products", "JSON_UNPARSEABLE_FRAGMENT", "REJECT", "Quarantine", "A JSON fragment that could not be parsed even after salvage"),
 ("products", "NEGATIVE_PRICE", "REJECT", "Quarantine", "Price < 0"),
 ("products", "MISSING_PRICE", "REJECT", "Quarantine", "Price blank / non-numeric"),
 ("products", "IS_ACTIVE_DEFAULTED", "FLAG", "Default FALSE", "is_active missing or null - not marketed until confirmed"),
 ("products", "PRODUCT_NAME_MISSING", "FLAG", "Keep existing / NULL", "Blank product name"),
 ("products", "CATEGORY_STANDARDIZED", "FLAG", "Fix", "Category casing/whitespace normalized"),
 ("transactions", "CONFLICTING_DUPLICATE_ID", "REJECT", "Quarantine all versions", "Same transaction_id, different payload within one batch - cannot tell which is true"),
 ("transactions", "MISSING_AMOUNT", "REJECT", "Quarantine", "Amount blank"),
 ("transactions", "INVALID_AMOUNT", "REJECT", "Quarantine", "Amount not numeric after removing 'Rs.' / commas"),
 ("transactions", "NEGATIVE_AMOUNT", "REJECT", "Quarantine", "Amount < 0 (refunds are signalled by is_refund, not sign)"),
 ("transactions", "ZERO_AMOUNT", "REJECT", "Quarantine", "Amount = 0"),
 ("transactions", "AMOUNT_REFORMATTED", "FLAG", "Fix", "Comma-formatted or 'Rs.'-prefixed amount parsed"),
 ("transactions", "INVALID_TIMESTAMP", "REJECT", "Quarantine", "Timestamp missing / unparseable"),
 ("transactions", "FUTURE_DATED", "REJECT", "Quarantine", "Timestamp after the processing (business) date"),
 ("transactions", "SOURCE_DATE_FUTURE", "FLAG", "Use timestamp", "Source 'date' column is after the processing date"),
 ("transactions", "DATE_TIMESTAMP_MISMATCH", "FLAG", "Use timestamp", "'date' disagrees with 'timestamp' under every format interpretation; timestamp (system generated) is authoritative"),
 ("transactions", "INVALID_STATUS", "REJECT", "Quarantine", "Status not mappable to Success / Failed / Pending"),
 ("transactions", "STATUS_STANDARDIZED", "FLAG", "Fix", "Typos/casing (Succes, sucess, Faild, FAILED...) mapped"),
 ("transactions", "PAYMENT_METHOD_STANDARDIZED", "FLAG", "Fix", "Casing / whitespace normalized (UPI, Cash, Card, NetBanking, Cheque)"),
 ("transactions", "IS_REFUND_DEFAULTED", "FLAG", "Default FALSE", "is_refund blank"),
 ("transactions", "NON_INR_REVIEW", "FLAG", "Keep, excluded from INR value KPIs", "Currency other than INR - flagged for business review"),
 ("transactions", "CURRENCY_DEFAULTED", "FLAG", "Default INR", "Currency blank"),
 ("transactions", "LATE_CORRECTION", "FLAG", "Update fact in place", "Re-uses an existing transaction_id with changed payload"),
 ("transactions", "ORPHAN_ACCOUNT", "REJECT", "Quarantine, retried every run", "account_id not in customer dimension"),
 ("transactions", "ORPHAN_BRANCH", "REJECT", "Quarantine, retried every run", "branch_id not in branch dimension"),
 ("transactions", "ORPHAN_PRODUCT", "REJECT", "Quarantine, retried every run", "product_id not in product dimension"),
]
(spark.createDataFrame(RULES, "entity string, code string, rule_type string, action string, description string")
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{AUDIT}.dq_rule_catalog"))
display(spark.table(f"{AUDIT}.dq_rule_catalog"))
