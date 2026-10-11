# Databricks notebook source
# MAGIC %md
# MAGIC # Common configuration & helpers
# MAGIC Shared by every pipeline notebook via `%run ./includes/common`.
# MAGIC
# MAGIC * **Parameters** (job / widget): `catalog`, `batch_id`, optional `business_date`, `source_folder`
# MAGIC * **Layers** (Unity Catalog schemas): `rb_landing` (Volume, raw files) → `rb_bronze` → `rb_silver` → `rb_gold`; `rb_audit` for pipeline metadata
# MAGIC * Written for **serverless** compute: no RDD / sparkContext / cache, ANSI-safe parsing (`try_*`)
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC Think of it as the **toolbox** of the project. It does not process data by itself. Every other notebook
# MAGIC starts with `%run ./includes/common`, which executes this file first, so all of them share:
# MAGIC 1. **Configuration** – which catalog / schemas / folders to use and which batch (day1, day2…) is running.
# MAGIC 2. **Parsing helpers** – clean strings, parse the 5 different date formats, normalise Indian phone numbers.
# MAGIC 3. **Audit helpers** – write load statistics and quarantined (rejected) records in one standard way.
# MAGIC 4. **SCD Type 2 helper** – one function that keeps history for dimension tables (used for customers, branches, products).
# MAGIC
# MAGIC Keeping this logic in one place means every layer applies the *same* rules, and a fix is made once.

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# 1. IMPORTS
# ---------------------------------------------------------------------------------------------
# Standard Python libraries: json/re for parsing text, hashlib for file checksums (md5),
# os/shutil for copying files inside Unity Catalog Volumes (Volumes behave like a normal folder).
import json, re, hashlib, os, shutil
from datetime import datetime, timedelta
from functools import reduce          # used to union a list of DataFrames into one

# PySpark: F = column functions (trim, when, sha2 ...), W = window specs (row_number, lag ...)
from pyspark.sql import functions as F, Window as W, DataFrame
from pyspark.sql.types import StringType, StructType, StructField
# DeltaTable gives us the MERGE (upsert) API used for SCD2 dimensions and the fact table.
from delta.tables import DeltaTable

# ---------------------------------------------------------------------------------------------
# 2. PARAMETERS (widgets)
# ---------------------------------------------------------------------------------------------
# Widgets are notebook input boxes. When the notebook runs inside a Databricks Job, the job
# parameters (e.g. batch_id=day2) are passed into these widgets automatically.
# The 3rd argument is the default value used when someone runs the notebook by hand.
dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_id", "day1", "Batch id")
dbutils.widgets.text("business_date", "", "Business date override (yyyy-MM-dd)")
dbutils.widgets.text("source_folder", "", "Landing folder override")

# Read the parameter values. `or "workspace"` protects against an empty value.
CATALOG = dbutils.widgets.get("catalog").strip() or "workspace"
BATCH_ID = dbutils.widgets.get("batch_id").strip()

# Fully qualified schema names, e.g. "workspace.rb_bronze". One schema per medallion layer:
#   landing = raw files (Volume) | bronze = raw rows as text | silver = cleansed
#   gold = star schema + KPIs    | audit = pipeline metadata (logs, profiles, DQ checks)
LANDING, BRONZE, SILVER, GOLD, AUDIT = (f"{CATALOG}.rb_{s}" for s in ("landing", "bronze", "silver", "gold", "audit"))
# Volumes are Unity Catalog governed folders. Files are read/written with normal file paths.
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/rb_landing/raw"          # where source drops land
ARCHIVE_VOLUME_PATH = f"/Volumes/{CATALOG}/rb_landing/archive"  # immutable copy of every ingested file

# Known batches. A new drop (e.g. day3) can be run without code changes by passing
# source_folder + business_date as job parameters.
#   seq           = order of the batches (Day 2 must come after Day 1)
#   folder        = sub-folder of the landing Volume that holds the files of this batch
#   business_date = the "as of" date of the batch. Used for: future-date checks,
#                   dormancy (last 90 days), SCD2 effective dates.
BATCH_REGISTRY = {
    "day1": {"seq": 1, "folder": "RetailBank_SourceData", "business_date": "2026-09-30"},
    "day2": {"seq": 2, "folder": "RetailBank_SourceData_Day2", "business_date": "2026-10-01"},
}

# Source files are matched by pattern so Day 1 / Day 2 naming differences need no config.
# e.g. "customers.csv" (Day 1) and "customer_updates_day2.csv" (Day 2) both match ^customer.*\.csv$
ENTITY_SOURCES = {
    "branches":     {"pattern": r"^branches.*\.csv$",            "format": "csv"},
    "customers":    {"pattern": r"^customer.*\.csv$",            "format": "csv"},
    "products":     {"pattern": r"^products.*\.(json|txt)$",     "format": "json_array"},
    "transactions": {"pattern": r"^transactions.*\.csv$",        "format": "csv"},
}

# Business columns per entity, in source order. Bronze keeps them all as STRING.
# (Reading everything as text means nothing is lost or silently converted at ingestion time;
#  types are applied later in silver where bad values can be detected and reported.)
SOURCE_COLUMNS = {
    "branches": ["branch_id", "branch_name", "location", "manager_name", "opened_date", "region", "branch_type", "contact_number"],
    "customers": ["customer_id", "customer_name", "email", "phone", "account_id", "gender", "dob", "address", "kyc_status", "registration_date", "transaction_id"],
    "products": ["product_id", "product_name", "product_type", "price", "launch_date", "is_active", "category", "vendor_name"],
    "transactions": ["transaction_id", "account_id", "product_id", "branch_id", "date", "timestamp", "amount", "payment_method", "status", "currency", "remarks", "is_refund"],
}

def raw_schema(entity):
    # Builds a Spark schema where every column of the entity is a STRING.
    # Example for branches: StructType([branch_id string, branch_name string, ...])
    return StructType([StructField(c, StringType()) for c in SOURCE_COLUMNS[entity]])

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# 3. RESOLVE THE CURRENT BATCH
# ---------------------------------------------------------------------------------------------
# Finds the metadata (folder, business date, sequence number) of the batch being processed.

def _batch_control_row(batch_id):
    # rb_audit.batch_control has one row per batch that has ever run. If the table does not exist
    # yet (very first run) or the batch is unknown, return None.
    if not spark.catalog.tableExists(f"{AUDIT}.batch_control"):
        return None
    rows = spark.table(f"{AUDIT}.batch_control").filter(F.col("batch_id") == batch_id).collect()
    return rows[0] if rows else None

def resolve_batch(batch_id):
    """Batch metadata from widgets > registry > batch_control table."""
    # Priority order for each value: explicit job parameter  >  BATCH_REGISTRY  >  previous run in batch_control
    reg = BATCH_REGISTRY.get(batch_id, {})
    ctl = _batch_control_row(batch_id)
    folder = dbutils.widgets.get("source_folder").strip() or reg.get("folder") or (ctl and ctl["source_folder"])
    bdate = dbutils.widgets.get("business_date").strip() or reg.get("business_date") or (ctl and str(ctl["business_date"]))
    if not folder or not bdate:
        # Fail fast with a clear message rather than processing the wrong folder.
        raise ValueError(f"Unknown batch '{batch_id}': pass source_folder and business_date parameters")
    seq = reg.get("seq") or (ctl and ctl["batch_seq"])
    if not seq:
        # A brand-new batch (e.g. day3) gets the next sequence number automatically.
        mx = spark.sql(f"SELECT coalesce(max(batch_seq), 0) m FROM {AUDIT}.batch_control").first()["m"]
        seq = mx + 1
    return {"batch_id": batch_id, "seq": int(seq), "folder": folder, "business_date": bdate}

# These three global variables are used by every notebook after %run.
BATCH = resolve_batch(BATCH_ID)
BUSINESS_DATE = BATCH["business_date"]   # e.g. "2026-10-01"
BATCH_SEQ = BATCH["seq"]                  # e.g. 2
print(f"catalog={CATALOG} batch={BATCH}")

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# 4. GENERIC COLUMN HELPERS (used by the silver cleansing notebooks)
# ---------------------------------------------------------------------------------------------

# ---------- generic column helpers ----------

def clean_str(c):
    """trim, collapse internal whitespace, empty -> NULL"""
    # Accepts a column name ("email") or a Column object.
    # Example: "  Manager_2  " -> "Manager_2" ;  "Brigade   Rd" -> "Brigade Rd" ;  "   " -> NULL
    col = c if not isinstance(c, str) else F.col(c)
    v = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.when(v == "", F.lit(None)).otherwise(v)

def codes(*pairs):
    """Build an array<string> of codes for which the condition is TRUE (NULL-safe)."""
    # Used for both reject reasons and data-quality flags. Example:
    #   codes((amount IS NULL, "MISSING_AMOUNT"), (amount < 0, "NEGATIVE_AMOUNT"))
    #   -> ["MISSING_AMOUNT"] for a blank amount, [] for a valid amount.
    # F.when(cond, code) gives the code when the condition is true and NULL otherwise;
    # F.filter then removes the NULLs so only the codes that apply remain.
    return F.filter(F.array(*[F.when(cond, F.lit(code)) for cond, code in pairs]), lambda x: x.isNotNull())

def row_hash(*cols):
    # A SHA-256 fingerprint of a set of columns. Two rows with the same values get the same hash,
    # which lets us detect "has anything changed?" cheaply (used for duplicates, SCD2 and corrections).
    # NULL is replaced by the text "<null>" so that NULL and "" do not produce the same hash.
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)

def tcast(colname, typ):
    # try_cast returns NULL instead of throwing an error when the text is not a valid number.
    # Serverless runs with ANSI SQL mode on, where a normal CAST('abc' AS INT) would fail the job.
    return F.expr(f"try_cast(`{colname}` AS {typ})")

def _safe_make_date(y, m, d):
    """make_date that returns NULL instead of failing for impossible dates (ANSI-safe)."""
    # Checks month 1-12, day >= 1, a sensible year, and that the day exists in that month
    # (last_day gives the month's last day, so 31-Feb is rejected). Only then builds the date.
    ok = m.between(1, 12) & (d >= 1) & y.between(1800, 2200)
    return F.when(ok, F.when(d <= F.dayofmonth(F.last_day(F.make_date(y, m, F.lit(1)))), F.make_date(y, m, d)))

def date_candidates(colname):
    """
    Returns (ymd, dmy, mdy, shape) columns for a raw date string.
    Supported shapes: yyyy-MM-dd, yyyy/MM/dd, dd-MM-yyyy, dd/MM/yyyy, MM/dd/yyyy (+ optional time part).
    """
    # The source files mix 5 date formats. Instead of guessing, we compute EVERY plausible reading:
    #   ymd = year-month-day   (2026-09-30, 2026/09/30)
    #   dmy = day-month-year   (30/09/2026, 30-09-2026)
    #   mdy = month-day-year   (09/30/2026)
    # and "shape" tells which pattern the text had. The caller then picks the right candidate.
    s = F.trim(F.split(F.trim(F.col(colname)), " ").getItem(0))          # drop a time part if present
    ymd_re, xxy_re = r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$"
    is_ymd, is_xxy = s.rlike(ymd_re), s.rlike(xxy_re)
    gi = lambda rx, i: F.regexp_extract(s, rx, i).cast("int")             # extract regex group i as int
    ymd = F.when(is_ymd, _safe_make_date(gi(ymd_re, 1), gi(ymd_re, 2), gi(ymd_re, 3)))
    a, b, y = gi(xxy_re, 1), gi(xxy_re, 2), gi(xxy_re, 3)                  # a/b/yyyy -> a and b are day or month
    dmy = F.when(is_xxy, _safe_make_date(y, b, a))
    mdy = F.when(is_xxy, _safe_make_date(y, a, b))
    shape = (F.when(s.rlike(r"^\d{4}-\d{1,2}-\d{1,2}$"), "yyyy-MM-dd")
              .when(s.rlike(r"^\d{4}/\d{1,2}/\d{1,2}$"), "yyyy/MM/dd")
              .when(s.rlike(r"^\d{1,2}-\d{1,2}-\d{4}$"), "dd-MM-yyyy")
              .when(s.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), "xx/xx/yyyy")    # could be dd/MM or MM/dd
              .when(s.isNull(), None).otherwise("UNPARSEABLE"))
    return ymd, dmy, mdy, shape

def with_parsed_date(df, colname, out):
    """
    Adds <out> (DATE), <out>_format and <out>_ambiguous.
    For dd/MM vs MM/dd: day>12 or month>12 disambiguates; otherwise Indian convention dd/MM/yyyy is assumed
    and the value is flagged ambiguous. dd-MM-yyyy (dash) is always day-first.
    """
    # Used for dimension dates (dob, registration_date, opened_date, launch_date) where there is no
    # second column to cross-check against. Rules, in order:
    #   1. ISO style (year first)          -> unambiguous
    #   2. dd-MM-yyyy with dashes          -> the files only use dashes for day-first
    #   3. first number > 12  (25/08/2026) -> must be the day   -> dd/MM/yyyy
    #   4. second number > 12 (08/25/2026) -> must be the day   -> MM/dd/yyyy
    #   5. both <= 12         (04/08/2026) -> assume dd/MM (Indian convention) and FLAG it as ambiguous
    ymd, dmy, mdy, shape = date_candidates(colname)
    s = F.trim(F.col(colname))
    first = F.regexp_extract(s, r"^(\d{1,2})[-/]", 1).cast("int")
    second = F.regexp_extract(s, r"^\d{1,2}[-/](\d{1,2})[-/]", 1).cast("int")
    resolved = (F.when(ymd.isNotNull(), ymd)
                 .when(shape == "dd-MM-yyyy", dmy)
                 .when(first > 12, dmy)
                 .when(second > 12, mdy)
                 .otherwise(dmy))
    # Human readable format that was detected (shown in data-quality reports).
    fmt = (F.when(shape == "xx/xx/yyyy", F.when(first > 12, "dd/MM/yyyy").when(second > 12, "MM/dd/yyyy").otherwise("dd/MM/yyyy (assumed)"))
            .otherwise(shape))
    # Ambiguous = both readings are valid and different (01/01 is not ambiguous: same date either way).
    amb = (shape == "xx/xx/yyyy") & (first <= 12) & (second <= 12) & (first != second)
    return (df.withColumn(out, resolved).withColumn(f"{out}_format", fmt)
              .withColumn(f"{out}_ambiguous", F.coalesce(amb, F.lit(False))))

def norm_indian_phone(colname):
    """Returns (normalized '+91-XXXXXXXXXX' or NULL, is_valid, was_changed)."""
    # Source phones arrive as "+91-9970629620", "9970629620", "+91 70985 93174", "919970629620",
    # or broken values like "+91-84858333" (only 8 digits). Target format: +91-XXXXXXXXXX.
    raw = F.trim(F.col(colname))
    digits = F.regexp_replace(raw, r"\D", "")                     # keep digits only
    with_cc = (F.length(digits) == 12) & digits.startswith("91")  # 91 + 10-digit number
    # '+..' means a country code is present: it must be +91 followed by exactly 10 digits
    # (so '+91-84858333' is invalid, not re-read as the 10-digit number 9184858333)
    ten = (F.when(raw.startswith("+"), F.when(with_cc, F.substring(digits, 3, 10)))
            .when(F.length(digits) == 10, digits)
            .when(with_cc, F.substring(digits, 3, 10)))
    val = F.when(ten.isNotNull(), F.concat(F.lit("+91-"), ten))   # NULL when the number is invalid
    # Returns three columns: the clean value, whether it is valid, whether we had to change the format.
    return val, val.isNotNull(), val.isNotNull() & (val != raw)

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# 5. TABLE / AUDIT HELPERS
# ---------------------------------------------------------------------------------------------

# ---------- table / audit helpers ----------

def table_exists(name):
    return spark.catalog.tableExists(name)

def overwrite_partition(df, table, col="_batch_id", value=None):
    """Idempotent write: replace only this batch's slice of the table."""
    # IDEMPOTENCY: re-running day2 must not create duplicates. Delta's "replaceWhere" deletes the rows
    # of THIS batch and writes the new ones in one atomic transaction; rows of other batches are untouched.
    # mergeSchema=true lets a later run add a new column without failing.
    value = value or BATCH_ID
    w = df.write.format("delta").option("mergeSchema", "true")
    if table_exists(table):
        w.mode("overwrite").option("replaceWhere", f"{col} = '{value}'").saveAsTable(table)
    else:
        w.mode("overwrite").saveAsTable(table)      # first run: create the table

def write_metrics(entity, metrics: dict, stage):
    """Key/value load statistics per batch (drives KPI 11 and the run summary)."""
    # Example call: write_metrics("customers", {"received": 47, "rejected": 0}, stage="SILVER")
    # Stored as rows (batch, stage, entity, metric, value) in rb_audit.load_stats.
    rows = [(BATCH_ID, BATCH_SEQ, stage, entity, k, int(v)) for k, v in metrics.items()]
    df = (spark.createDataFrame(rows, "batch_id string, batch_seq int, stage string, entity string, metric string, value long")
          .withColumn("recorded_at", F.current_timestamp()))
    # Delete-then-insert = a re-run replaces its own numbers instead of adding a second copy.
    spark.sql(f"DELETE FROM {AUDIT}.load_stats WHERE batch_id = '{BATCH_ID}' AND entity = '{entity}' AND stage = '{stage}'")
    df.write.mode("append").saveAsTable(f"{AUDIT}.load_stats")
    print(f"[{stage}] {entity}: {metrics}")

# Duplicates are "removed" rather than "invalid": they are logged with severity INFO instead of ERROR.
INFO_REASONS = ["DUPLICATE_EXACT", "DUPLICATE_SUPERSEDED"]

def write_quarantine(rejects: DataFrame, entity, stage):
    """
    rejects must carry: _batch_id, _source_file, _row_num, record_key, reason_codes (array<string>), _raw (struct)
    Every rejected record keeps its untouched raw payload + all reason codes -> nothing is silently dropped.
    """
    # The quarantine table is the "rejects path" of the brief. For every rejected row we keep:
    #   where it came from (file + line number), WHY it was rejected (all reason codes),
    #   and the ORIGINAL row as JSON so it can be inspected, fixed at the source, or re-processed later.
    out = (rejects
        .select(
            F.col("_batch_id").alias("batch_id"),
            F.lit(BATCH_SEQ).alias("batch_seq"),
            F.lit(entity).alias("entity"),
            F.lit(stage).alias("stage"),                                   # BRONZE (unparseable) or SILVER (failed rules)
            F.col("_source_file").alias("source_file"),
            F.col("_row_num").cast("long").alias("source_row_num"),
            F.col("record_key"),                                           # e.g. the transaction_id
            F.col("reason_codes")[0].alias("reason_code"),                 # primary (first) reason
            F.col("reason_codes"),                                         # all reasons
            F.when(F.col("reason_codes")[0].isin(INFO_REASONS), "INFO").otherwise("ERROR").alias("severity"),
            F.to_json(F.col("_raw")).alias("raw_record"),                  # untouched source values
            F.current_timestamp().alias("quarantined_at"),
            F.lit(None).cast("string").alias("resolved_batch_id"),         # filled if a later batch fixes it
            F.lit(None).cast("string").alias("resolution")))
    out.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{SILVER}.quarantine")
    return out

def reset_quarantine(entity, stage):
    """Make a re-run of the same batch idempotent."""
    # Before a notebook writes this batch's rejects, it removes the rejects of a previous run of the
    # same batch/entity/stage, so re-running never doubles the quarantine.
    if table_exists(f"{SILVER}.quarantine"):
        spark.sql(f"""DELETE FROM {SILVER}.quarantine
                      WHERE batch_id = '{BATCH_ID}' AND entity = '{entity}' AND stage = '{stage}'""")

# COMMAND ----------

# ---------------------------------------------------------------------------------------------
# 6. SCD TYPE 2 UPSERT
# ---------------------------------------------------------------------------------------------
# SCD = Slowly Changing Dimension.
#   Type 1 = overwrite the old value (no history)        -> used for corrections like a phone format
#   Type 2 = keep the old row, close it, add a new row   -> used for business changes like KYC status
# Each dimension row therefore has: version, effective_from, effective_to, is_current.
#
# Example (customer C064 on Day 2, KYC Pending -> Verified):
#   version 1 | Pending  | 1900-01-01 -> 2026-10-01 | is_current = false   (closed)
#   version 2 | Verified | 2026-10-01 -> 9999-12-31 | is_current = true    (new)

# ---------- SCD Type 2 upsert (atomic single MERGE) ----------

SCD_EFFECTIVE_START = "1900-01-01 00:00:00"   # first version is valid "since forever" so old transactions find it
SCD_EFFECTIVE_END = "9999-12-31 23:59:59"     # "open ended" end date of the current version

def scd2_upsert(target, src: DataFrame, key, scd2_cols, type1_cols):
    """
    * NEW key            -> insert version 1 (effective from 1900-01-01 so historical facts resolve)
    * scd2 attribute diff -> expire current row (effective_to = business date) + insert new version
    * type1 attribute diff -> update current row in place
    * identical          -> no-op (makes re-runs idempotent)
    NULL incoming attributes never overwrite a known value (partial update feeds).
    Returns change counts.
    """
    attrs = scd2_cols + type1_cols

    # STEP 1 - put each incoming row next to the CURRENT version of the same key (if any).
    cur = spark.table(target).filter("is_current")
    s = src.alias("s").join(cur.alias("c"), F.col(f"s.{key}") == F.col(f"c.{key}"), "left")
    sel = [F.col(f"s.{key}").alias(key)]
    for c in attrs:
        # coalesce(new, current): a blank value in the update feed keeps the known value
        # (e.g. Day-2 product P402 arrives without a name -> the name is not erased).
        sel.append(F.coalesce(F.col(f"s.{c}"), F.col(f"c.{c}")).alias(c))
    sel += [F.col(f"c.{key}").isNotNull().alias("_exists"), F.col("c.hash_scd2").alias("_cur_h2"),
            F.col("c.hash_type1").alias("_cur_h1"), F.col("c.version").alias("_cur_version")]

    # STEP 2 - classify every incoming row by comparing hashes (fingerprints) of the two column groups.
    staged = (s.select(*sel)
              .withColumn("hash_scd2", row_hash(*scd2_cols))
              .withColumn("hash_type1", row_hash(*type1_cols))
              .withColumn("_change_type",
                  F.when(~F.col("_exists"), "NEW")                                       # key never seen
                   .when(F.col("hash_scd2") != F.col("_cur_h2"), "SCD2_CHANGE")          # history-worthy change
                   .when(F.col("hash_type1") != F.col("_cur_h1"), "TYPE1_CHANGE")        # simple correction
                   .otherwise("UNCHANGED")))                                             # nothing to do

    # Counts are returned to the caller and stored in rb_audit.load_stats.
    counts = {r["_change_type"]: r["n"] for r in staged.groupBy("_change_type").agg(F.count("*").alias("n")).collect()}
    changes = staged.filter("_change_type <> 'UNCHANGED'")

    # STEP 3 - the "staged union" trick that lets ONE MERGE both close the old row and insert the new one.
    # An SCD2 change appears twice in merge_src:
    #   copy 1: _merge_key = key  -> MATCHES the current row  -> closes it (is_current=false)
    #   copy 2: _merge_key = NULL -> matches nothing          -> INSERTED as the new version
    # NEW rows only have copy 1, which matches nothing and is inserted. TYPE1 rows match and are updated.
    merge_src = (changes.withColumn("_merge_key", F.col(key))
                 .unionByName(changes.filter("_change_type = 'SCD2_CHANGE'").withColumn("_merge_key", F.lit(None).cast("string"))))

    eff_ts = f"CAST('{BUSINESS_DATE} 00:00:00' AS TIMESTAMP)"   # a change becomes effective at the batch date
    insert_vals = {key: f"s.{key}", **{c: f"s.{c}" for c in attrs},
        "hash_scd2": "s.hash_scd2", "hash_type1": "s.hash_type1",
        "version": "coalesce(s._cur_version, 0) + 1",           # 1 for new keys, current+1 for changes
        "effective_from": f"CASE WHEN s._change_type = 'NEW' THEN CAST('{SCD_EFFECTIVE_START}' AS TIMESTAMP) ELSE {eff_ts} END",
        "effective_to": f"CAST('{SCD_EFFECTIVE_END}' AS TIMESTAMP)",
        "is_current": "true",
        "created_batch_id": f"'{BATCH_ID}'", "updated_batch_id": f"'{BATCH_ID}'",
        "created_at": "current_timestamp()", "updated_at": "current_timestamp()"}
        # (the surrogate key column, e.g. customer_sk, is an IDENTITY column: Delta generates it automatically)

    # STEP 4 - one atomic MERGE: either everything is applied or nothing (no half-updated dimension).
    (DeltaTable.forName(spark, target).alias("t")
        .merge(merge_src.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        # close the current version of a key that has an SCD2 change
        .whenMatchedUpdate(condition="s._change_type = 'SCD2_CHANGE'", set={
            "is_current": "false", "effective_to": eff_ts,
            "updated_batch_id": f"'{BATCH_ID}'", "updated_at": "current_timestamp()"})
        # overwrite Type-1 columns in place (no new version)
        .whenMatchedUpdate(condition="s._change_type = 'TYPE1_CHANGE'", set={
            **{c: f"s.{c}" for c in type1_cols}, "hash_type1": "s.hash_type1",
            "updated_batch_id": f"'{BATCH_ID}'", "updated_at": "current_timestamp()"})
        # insert brand-new keys and the new version of SCD2-changed keys
        .whenNotMatchedInsert(condition="s._change_type IN ('NEW', 'SCD2_CHANGE')", values=insert_vals)
        .execute())
    return {"received": sum(counts.values()), "new": counts.get("NEW", 0), "scd2_changed": counts.get("SCD2_CHANGE", 0),
            "type1_updated": counts.get("TYPE1_CHANGE", 0), "unchanged": counts.get("UNCHANGED", 0)}
