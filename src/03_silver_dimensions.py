# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver – cleanse & validate dimensions (branches, customers, products)
# MAGIC For each entity: **standardize → validate (reject with reason) → de-duplicate → write** `rb_silver.<entity>`.
# MAGIC Every fix is recorded in a `dq_flags` array on the row; every rejected record lands in `rb_silver.quarantine`
# MAGIC with its raw payload and reason codes. See `rb_audit.dq_rule_catalog` for the full rule list.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC Silver turns raw text into **trusted, typed, standardised** records. Each entity follows the same 4 steps:
# MAGIC 1. **Standardize** – trim spaces, fix casing (`NORTH` → `North`), parse dates, normalise phones, repair emails.
# MAGIC 2. **Validate** – a record that cannot be trusted gets one or more *reason codes* (e.g. `MISSING_ACCOUNT_ID`).
# MAGIC 3. **De-duplicate** – exact re-sends are dropped (logged as `DUPLICATE_EXACT`); for the same key with
# MAGIC    different content the best copy is kept (`DUPLICATE_SUPERSEDED`).
# MAGIC 4. **Write** – good rows to `rb_silver.<entity>`, rejected rows to `rb_silver.quarantine`, counts to `rb_audit.load_stats`.
# MAGIC
# MAGIC Two different outputs per row: **reason_codes** (= reject, row is quarantined) and **dq_flags**
# MAGIC (= we repaired or noted something, but the row is still good and continues to gold).

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# The batch business date as a Spark date literal (used for "date in the future?" checks).
BD = F.lit(BUSINESS_DATE).cast("date")
EMAIL_DOMAINS = ["example.com"]          # known domains used to repair a missing '@'
# A simple, standard email shape: something@domain.tld
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

def bronze(entity):
    # Read this batch's raw rows and keep an untouched copy of all business columns in a struct "_raw".
    # _raw travels with the row through every step, so a rejected row can be stored exactly as it arrived.
    return (spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID)
            .withColumn("_raw", F.struct(*SOURCE_COLUMNS[entity])))

def date_flags(prefix):
    # Two standard flags for any parsed date column (see with_parsed_date in includes/common):
    #   DATE_FORMAT_STANDARDIZED : the source was not yyyy-MM-dd and had to be converted
    #   DATE_AMBIGUOUS_DDMM      : dd/MM vs MM/dd could not be decided; dd/MM was assumed
    return [(~F.col(f"{prefix}_format").isin("yyyy-MM-dd") & F.col(f"{prefix}_format").isNotNull(), "DATE_FORMAT_STANDARDIZED"),
            (F.col(f"{prefix}_ambiguous"), "DATE_AMBIGUOUS_DDMM")]

def split_and_dedupe(df, key, payload_cols):
    """
    1) rows with reject reasons -> rejected
    2) identical payload for same key -> keep first, rest DUPLICATE_EXACT
    3) same key, different payload -> keep best (most complete, fewest fixes, latest in feed), rest DUPLICATE_SUPERSEDED
    """
    # Step 1: separate invalid rows (they have at least one reason code).
    bad = df.filter(F.size("reason_codes") > 0)
    # Fingerprint of the CLEANED business columns. Because it is computed after cleaning,
    # "Customer_4  " / "CUSTOMER_4@EXAMPLE.COM" and "Customer_4" / "customer_4@example.com" get the same hash
    # -> near-duplicates (whitespace / casing differences) are caught as exact duplicates.
    ok = df.filter(F.size("reason_codes") == 0).withColumn("_payload_hash", row_hash(*payload_cols))
    # Step 2: number the copies of the same key + same content in file order; copy 2, 3 ... are duplicates.
    ok = ok.withColumn("_rn_exact", F.row_number().over(W.partitionBy(key, "_payload_hash").orderBy("_row_num")))
    exact = ok.filter("_rn_exact > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_EXACT")))
    uniq = ok.filter("_rn_exact = 1")
    # Step 3: same key but DIFFERENT content -> choose the best version with a score:
    #   +10 per filled column (more complete wins), -1 per fix that was needed (cleaner source wins),
    #   and on a tie the LATER row in the file wins (a re-sent master record is the newer one).
    score = reduce(lambda a, b: a + b, [F.col(c).isNotNull().cast("int") for c in payload_cols]) * 10 - F.size("dq_flags")
    uniq = uniq.withColumn("_rn_best", F.row_number().over(W.partitionBy(key).orderBy(score.desc(), F.col("_row_num").desc())))
    superseded = uniq.filter("_rn_best > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_SUPERSEDED")))
    kept = uniq.filter("_rn_best = 1").drop("_rn_exact", "_rn_best", "_payload_hash")
    # Everything that is not kept is returned as "rejected" so it can be written to quarantine.
    rejected = reduce(DataFrame.unionByName, [bad, exact.drop("_rn_exact", "_payload_hash"),
                                             superseded.drop("_rn_exact", "_rn_best", "_payload_hash")])
    return kept, rejected

def finish(entity, key, kept, rejected, out_cols):
    # Common last step for every entity: write quarantine, write silver, record the counts.
    reset_quarantine(entity, "SILVER")                                    # re-run safety
    write_quarantine(rejected.withColumn("record_key", F.col(key)), entity, "SILVER")
    out = kept.select(*out_cols, "dq_flags", "_batch_id", "_batch_seq", "_source_file", "_row_num")
    overwrite_partition(out, f"{SILVER}.{entity}")                        # replace only this batch's rows
    # Reconciliation numbers (received = passed + rejected is asserted later in 07_dq_checks).
    received = spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID).count()
    passed = spark.table(f"{SILVER}.{entity}").filter(F.col("_batch_id") == BATCH_ID).count()
    q = spark.table(f"{SILVER}.quarantine").filter((F.col("batch_id") == BATCH_ID) & (F.col("entity") == entity) & (F.col("stage") == "SILVER"))
    write_metrics(entity, {"received": received, "passed": passed, "rejected": q.count(),
                           "passed_with_fixes": spark.table(f"{SILVER}.{entity}").filter((F.col("_batch_id") == BATCH_ID) & (F.size("dq_flags") > 0)).count()},
                  stage="SILVER")
    display(q.groupBy("reason_code").count().orderBy(F.desc("count")))   # quick view of why rows were rejected

# COMMAND ----------

# MAGIC %md ## Branches
# MAGIC Issues found by profiling: 3 re-sent branches (B002, B012, B051), blank managers, region in 8 spellings
# MAGIC (`NORTH`, `south `, ` West `), branch_type casing, 4 non-ISO opened dates, 5 phone layouts.

# COMMAND ----------

b = bronze("branches")
b = with_parsed_date(b, "opened_date", "_opened")                        # adds _opened, _opened_format, _opened_ambiguous
contact, contact_ok, contact_changed = norm_indian_phone("contact_number")
# "  NORTH " -> "North" ; "RURAL" -> "Rural"
region_c, type_c = F.initcap(F.lower(clean_str("region"))), F.initcap(F.lower(clean_str("branch_type")))
b = (b.select(
        F.upper(clean_str("branch_id")).alias("branch_id"),
        clean_str("branch_name").alias("branch_name"),
        F.initcap(clean_str("location")).alias("location"),
        clean_str("manager_name").alias("manager_name"),                   # blank -> NULL (flagged below)
        F.col("_opened").alias("opened_date"),
        # only accept known values; anything else becomes NULL and is flagged REGION_INVALID
        F.when(region_c.isin("North", "South", "East", "West", "Central"), region_c).alias("region"),
        F.when(type_c.isin("Urban", "Rural", "Semi-Urban", "Metro"), type_c).alias("branch_type"),
        contact.alias("contact_number"),
        # helper columns (prefix _) used only to decide the flags; they are not written to silver
        F.col("_opened_format"), F.col("_opened_ambiguous"),
        (F.col("region") != region_c).alias("_region_fixed"), (F.col("branch_type") != type_c).alias("_type_fixed"),
        contact_ok.alias("_contact_ok"), contact_changed.alias("_contact_changed"), F.col("contact_number").alias("_contact_raw"),
        "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num"))
# Reject rule: a branch id must look like B + digits. Flags: everything we repaired or noticed.
b = (b.withColumn("reason_codes", codes((F.col("branch_id").isNull() | ~F.col("branch_id").rlike(r"^B\d+$"), "INVALID_KEY")))
      .withColumn("dq_flags", codes(
          (F.col("manager_name").isNull(), "MANAGER_MISSING"),
          (F.col("_region_fixed"), "REGION_STANDARDIZED"),
          (F.col("region").isNull(), "REGION_INVALID"),
          (F.col("_type_fixed"), "BRANCH_TYPE_STANDARDIZED"),
          (F.col("_contact_changed"), "PHONE_STANDARDIZED"),
          (~F.col("_contact_ok") & F.col("_contact_raw").isNotNull(), "PHONE_INVALID"),
          *date_flags("_opened"))))
BRANCH_COLS = ["branch_id", "branch_name", "location", "manager_name", "opened_date", "region", "branch_type", "contact_number"]
kept, rejected = split_and_dedupe(b, "branch_id", BRANCH_COLS)
finish("branches", "branch_id", kept, rejected, BRANCH_COLS)

# COMMAND ----------

# MAGIC %md ## Customers
# MAGIC Issues found: exact re-sends differing only in whitespace/casing, customers without account_id,
# MAGIC one account claimed by two customers (A0046), emails with `@@` / missing `@` / blanks, 6 phone layouts,
# MAGIC gender in 9 spellings, KYC in 8 spellings, DOB in the future (2028) or implausible (1901),
# MAGIC registration_date in 4 formats, and the same person appearing under several customer ids.

# COMMAND ----------

c = bronze("customers")
c = with_parsed_date(c, "dob", "_dob")
c = with_parsed_date(c, "registration_date", "_reg")
phone, phone_ok, phone_changed = norm_indian_phone("phone")

# EMAIL REPAIR in 3 small steps:
#   e0: lower-case + trim                            "CUSTOMER_4@EXAMPLE.COM " -> "customer_4@example.com"
#   e1: collapse "@@" into "@"                        "customer_99@@example.com" -> "customer_99@example.com"
#   e2: insert a missing "@" before a known domain    "customer_165example.com" -> "customer_165@example.com"
e0 = F.lower(clean_str("email"))
e1 = F.regexp_replace(e0, "@+", "@")
dom_rx = "(" + "|".join(re.escape(d) for d in EMAIL_DOMAINS) + ")$"
e2 = F.when(~e1.contains("@") & e1.rlike(dom_rx), F.regexp_replace(e1, dom_rx, "@$1")).otherwise(e1)
# GENDER: M / MALE / male -> Male ; F / female -> Female ; blank / Unknown -> Unknown
g = F.lower(clean_str("gender"))
gender = (F.when(g.isin("m", "male"), "Male").when(g.isin("f", "female"), "Female")
           .when(g.isin("o", "other"), "Other").otherwise("Unknown"))
# KYC: " Verified", "pending ", "PENDING" -> Verified / Pending / Rejected
k = F.lower(clean_str("kyc_status"))
kyc = F.when(k.isin("verified", "pending", "rejected"), F.initcap(k)).otherwise("Unknown")
# ADDRESS: an all-lower-case address is title-cased ("232 brigade rd, chennai" -> "232 Brigade Rd, Chennai");
# "Mg Road" produced by title-casing is turned back into "MG Road".
addr = clean_str("address")
addr = F.regexp_replace(F.when(addr == F.lower(addr), F.initcap(addr)).otherwise(addr), r"\bMg\b", "MG")

c = (c.select(
        F.upper(clean_str("customer_id")).alias("customer_id"),
        clean_str("customer_name").alias("customer_name"),
        F.when(e2.rlike(EMAIL_RE), e2).alias("email"),                    # still invalid after repair -> NULL
        phone.alias("phone"),
        F.upper(clean_str("account_id")).alias("account_id"),
        gender.alias("gender"),
        # DOB is kept only if it is not in the future and the age is at most 100 years (1200 months)
        F.when((F.col("_dob") <= BD) & (F.col("_dob") >= F.add_months(BD, -1200)), F.col("_dob")).alias("dob"),
        addr.alias("address"),
        F.initcap(F.trim(F.substring_index(addr, ",", -1))).alias("city"),  # text after the last comma
        kyc.alias("kyc_status"),
        F.col("_reg").alias("registration_date"),
        F.upper(clean_str("transaction_id")).alias("ref_transaction_id"),
        # helper columns for the flags below
        e0.alias("_email_in"), e2.alias("_email_fixed"), phone_ok.alias("_phone_ok"), phone_changed.alias("_phone_changed"),
        F.col("phone").alias("_phone_raw"), clean_str("gender").alias("_gender_raw"), clean_str("kyc_status").alias("_kyc_raw"),
        F.col("_dob").alias("_dob_parsed"), F.col("dob").alias("_dob_raw"),
        "_dob_format", "_dob_ambiguous", "_reg_format", "_reg_ambiguous",
        "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num"))
# REJECT rules: invalid customer id, or no usable account id (a customer without account cannot be
# linked to any transaction). FLAGS: every repair, so the DQ report can count them.
c = (c.withColumn("reason_codes", codes(
          (F.col("customer_id").isNull() | ~F.col("customer_id").rlike(r"^C\d+$"), "INVALID_KEY"),
          (F.col("account_id").isNull(), "MISSING_ACCOUNT_ID"),
          (F.col("account_id").isNotNull() & ~F.col("account_id").rlike(r"^A\d+$"), "INVALID_KEY")))
      .withColumn("dq_flags", codes(
          (F.col("_email_in").isNull(), "EMAIL_MISSING"),
          (F.col("email").isNotNull() & (F.col("_email_fixed") != F.col("_email_in")), "EMAIL_REPAIRED"),
          (F.col("_email_in").isNotNull() & F.col("email").isNull(), "EMAIL_INVALID"),
          (F.col("_phone_changed"), "PHONE_STANDARDIZED"),
          (~F.col("_phone_ok"), "PHONE_INVALID"),
          (F.col("_gender_raw").isNull() | (F.col("_gender_raw") != F.col("gender")), "GENDER_STANDARDIZED"),
          (F.col("_kyc_raw").isNull() | (F.col("_kyc_raw") != F.col("kyc_status")), "KYC_STANDARDIZED"),
          (F.col("_dob_raw").isNull(), "DOB_MISSING"),
          (F.col("_dob_parsed") > BD, "DOB_FUTURE"),
          (F.col("_dob_parsed") < F.add_months(BD, -1200), "DOB_IMPLAUSIBLE"),
          (F.col("registration_date") > BD, "REGISTRATION_DATE_FUTURE"),
          *date_flags("_dob"), *date_flags("_reg"))))
CUSTOMER_COLS = ["customer_id", "customer_name", "email", "phone", "account_id", "gender", "dob", "address", "city",
                 "kyc_status", "registration_date", "ref_transaction_id"]
kept, rejected = split_and_dedupe(c, "customer_id", CUSTOMER_COLS)

# --- one account_id claimed by several customers -------------------------------------------------
# Example: C046 and C8252 both claim account A0046. A transaction on A0046 must belong to ONE customer.
# Owner = (1) the customer whose number matches the account number (C046 <-> A0046),
#         (2) else the customer that already owns it in gold, (3) else the earliest registration.
# The other claimant is quarantined with SHARED_ACCOUNT_ID.
num = lambda colname: F.when(F.regexp_extract(F.col(colname), r"(\d+)", 1) != "", F.regexp_extract(F.col(colname), r"(\d+)", 1).cast("int"))
batch_side = kept.select("customer_id", "account_id", "registration_date", F.lit(False).alias("_existing"))
# Also consider customers already in gold (important on Day 2: a new customer cannot steal an existing account).
gold_side = (spark.table(f"{GOLD}.dim_customer").filter("is_current")
             .join(kept.select("customer_id"), "customer_id", "left_anti")
             .select("customer_id", "account_id", "registration_date", F.lit(True).alias("_existing")))
claims = batch_side.unionByName(gold_side)
owner_w = W.partitionBy("account_id").orderBy((num("customer_id") == num("account_id")).desc_nulls_last(),
                                             F.col("_existing").desc(), F.col("registration_date").asc_nulls_last(), "customer_id")
claims = (claims.withColumn("_claimants", F.count("*").over(W.partitionBy("account_id")))
                .withColumn("_owner_rank", F.row_number().over(owner_w)))
losers = claims.filter("_claimants > 1 AND _owner_rank > 1 AND NOT _existing").select("customer_id")
shared = kept.join(losers, "customer_id", "left_semi").withColumn("reason_codes", F.array(F.lit("SHARED_ACCOUNT_ID")))
kept = kept.join(losers, "customer_id", "left_anti")
rejected = rejected.unionByName(shared)

# --- likely same person under several customer_ids (name + dob) ------------------------------------
# e.g. Customer_143 born 1959-06-04 appears as C143, C1183, C2198, C3210. They have DIFFERENT accounts with
# their own transactions, so they are not merged (that is a business/MDM decision); instead each row gets
# potential_duplicate_of = the lowest other customer_id of the same person + flag POTENTIAL_DUPLICATE_PERSON.
people = (kept.select("customer_id", "customer_name", "dob")
          .unionByName(spark.table(f"{GOLD}.dim_customer").filter("is_current")
                       .join(kept.select("customer_id"), "customer_id", "left_anti").select("customer_id", "customer_name", "dob"))
          .filter(F.col("dob").isNotNull())
          .withColumn("_pk", F.concat_ws("|", F.lower("customer_name"), F.col("dob").cast("string"))))   # "person key"
people = (people.withColumn("_ids", F.collect_set("customer_id").over(W.partitionBy("_pk")))
                .withColumn("potential_duplicate_of", F.array_min(F.filter("_ids", lambda x: x != F.col("customer_id"))))
                .select("customer_id", "potential_duplicate_of"))
kept = (kept.join(people, "customer_id", "left")
            .withColumn("dq_flags", F.when(F.col("potential_duplicate_of").isNotNull(),
                                           F.array_union("dq_flags", F.array(F.lit("POTENTIAL_DUPLICATE_PERSON")))).otherwise(F.col("dq_flags"))))
finish("customers", "customer_id", kept, rejected, CUSTOMER_COLS + ["potential_duplicate_of"])

# COMMAND ----------

# MAGIC %md ## Products
# MAGIC Issues found: the JSON structural defect (handled in bronze), P002/P063/P089 re-sent later in the feed with a
# MAGIC different price, 3 negative prices (P298–P300), `is_active` as boolean / string / null / missing,
# MAGIC category casing and trailing spaces, a blank product name.

# COMMAND ----------

p = bronze("products")
p = with_parsed_date(p, "launch_date", "_launch")
# PRICE: remove "Rs.", "INR", commas and spaces, then try_cast to a decimal (NULL if still not a number).
price_txt = F.regexp_replace(clean_str("price"), r"(?i)rs\.?|inr|,|\s", "")
p = p.withColumn("_price_txt", price_txt).withColumn("_price", tcast("_price_txt", "DECIMAL(18,2)"))
# IS_ACTIVE: true/"true"/"True"/1/yes -> True ; false/"FALSE"/0/no -> False ;
# missing or null -> False (assumption: do not market a product whose status is unknown) + flag.
a = F.lower(clean_str("is_active"))
active = F.when(a.isin("true", "1", "yes", "y", "t"), True).when(a.isin("false", "0", "no", "n", "f"), False).otherwise(False)
# CATEGORY: "SME" stays upper-case, "RETAIL"/"Corporate " -> "Retail"/"Corporate"
cat_u = F.upper(clean_str("category"))
category = F.when(cat_u == "SME", "SME").otherwise(F.initcap(F.lower(clean_str("category"))))
ptype = F.initcap(F.lower(clean_str("product_type")))                  # "credit card" -> "Credit Card"
p = (p.select(
        F.upper(clean_str("product_id")).alias("product_id"),
        clean_str("product_name").alias("product_name"),
        ptype.alias("product_type"),
        F.col("_price").alias("price"),
        F.col("_launch").alias("launch_date"),
        active.alias("is_active"),
        category.alias("category"),
        clean_str("vendor_name").alias("vendor_name"),
        a.alias("_active_raw"), F.col("_is_active_source_type"), F.col("category").alias("_cat_raw"),
        "_launch_format", "_launch_ambiguous", "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num"))
# REJECT: bad id, no price, negative price. FLAG: defaults and standardisations.
p = (p.withColumn("reason_codes", codes(
          (F.col("product_id").isNull() | ~F.col("product_id").rlike(r"^P\d+$"), "INVALID_KEY"),
          (F.col("price").isNull(), "MISSING_PRICE"),
          (F.col("price") < 0, "NEGATIVE_PRICE")))
      .withColumn("dq_flags", codes(
          (F.col("_active_raw").isNull() | ~F.col("_active_raw").isin("true", "1", "yes", "y", "t", "false", "0", "no", "n", "f"), "IS_ACTIVE_DEFAULTED"),
          (F.col("_is_active_source_type") == "str", "IS_ACTIVE_STANDARDIZED"),
          (F.col("product_name").isNull(), "PRODUCT_NAME_MISSING"),
          (F.col("_cat_raw") != F.col("category"), "CATEGORY_STANDARDIZED"),
          (F.col("price") == 0, "ZERO_PRICE"),
          *date_flags("_launch"))))
PRODUCT_COLS = ["product_id", "product_name", "product_type", "price", "launch_date", "is_active", "category", "vendor_name"]
# For re-sent products (P002 twice with different prices) the later copy in the feed wins (see the score
# in split_and_dedupe): the file is a concatenated feed, so the later object is the newer one.
kept, rejected = split_and_dedupe(p, "product_id", PRODUCT_COLS)
finish("products", "product_id", kept, rejected, PRODUCT_COLS)
