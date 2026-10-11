# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver – cleanse & validate dimensions (branches, customers, products)
# MAGIC For each entity: **standardize → validate (reject with reason) → de-duplicate → write** `rb_silver.<entity>`.
# MAGIC Every fix is recorded in a `dq_flags` array on the row; every rejected record lands in `rb_silver.quarantine`
# MAGIC with its raw payload and reason codes. See `rb_audit.dq_rule_catalog` for the full rule list.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

BD = F.lit(BUSINESS_DATE).cast("date")
EMAIL_DOMAINS = ["example.com"]          # known domains used to repair a missing '@'
EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

def bronze(entity):
    return (spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID)
            .withColumn("_raw", F.struct(*SOURCE_COLUMNS[entity])))

def date_flags(prefix):
    return [(~F.col(f"{prefix}_format").isin("yyyy-MM-dd") & F.col(f"{prefix}_format").isNotNull(), "DATE_FORMAT_STANDARDIZED"),
            (F.col(f"{prefix}_ambiguous"), "DATE_AMBIGUOUS_DDMM")]

def split_and_dedupe(df, key, payload_cols):
    """
    1) rows with reject reasons -> rejected
    2) identical payload for same key -> keep first, rest DUPLICATE_EXACT
    3) same key, different payload -> keep best (most complete, fewest fixes, latest in feed), rest DUPLICATE_SUPERSEDED
    """
    bad = df.filter(F.size("reason_codes") > 0)
    ok = df.filter(F.size("reason_codes") == 0).withColumn("_payload_hash", row_hash(*payload_cols))
    ok = ok.withColumn("_rn_exact", F.row_number().over(W.partitionBy(key, "_payload_hash").orderBy("_row_num")))
    exact = ok.filter("_rn_exact > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_EXACT")))
    uniq = ok.filter("_rn_exact = 1")
    score = reduce(lambda a, b: a + b, [F.col(c).isNotNull().cast("int") for c in payload_cols]) * 10 - F.size("dq_flags")
    uniq = uniq.withColumn("_rn_best", F.row_number().over(W.partitionBy(key).orderBy(score.desc(), F.col("_row_num").desc())))
    superseded = uniq.filter("_rn_best > 1").withColumn("reason_codes", F.array(F.lit("DUPLICATE_SUPERSEDED")))
    kept = uniq.filter("_rn_best = 1").drop("_rn_exact", "_rn_best", "_payload_hash")
    rejected = reduce(DataFrame.unionByName, [bad, exact.drop("_rn_exact", "_payload_hash"),
                                             superseded.drop("_rn_exact", "_rn_best", "_payload_hash")])
    return kept, rejected

def finish(entity, key, kept, rejected, out_cols):
    reset_quarantine(entity, "SILVER")
    write_quarantine(rejected.withColumn("record_key", F.col(key)), entity, "SILVER")
    out = kept.select(*out_cols, "dq_flags", "_batch_id", "_batch_seq", "_source_file", "_row_num")
    overwrite_partition(out, f"{SILVER}.{entity}")
    received = spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID).count()
    passed = spark.table(f"{SILVER}.{entity}").filter(F.col("_batch_id") == BATCH_ID).count()
    q = spark.table(f"{SILVER}.quarantine").filter((F.col("batch_id") == BATCH_ID) & (F.col("entity") == entity) & (F.col("stage") == "SILVER"))
    write_metrics(entity, {"received": received, "passed": passed, "rejected": q.count(),
                           "passed_with_fixes": spark.table(f"{SILVER}.{entity}").filter((F.col("_batch_id") == BATCH_ID) & (F.size("dq_flags") > 0)).count()},
                  stage="SILVER")
    display(q.groupBy("reason_code").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md ## Branches

# COMMAND ----------

b = bronze("branches")
b = with_parsed_date(b, "opened_date", "_opened")
contact, contact_ok, contact_changed = norm_indian_phone("contact_number")
region_c, type_c = F.initcap(F.lower(clean_str("region"))), F.initcap(F.lower(clean_str("branch_type")))
b = (b.select(
        F.upper(clean_str("branch_id")).alias("branch_id"),
        clean_str("branch_name").alias("branch_name"),
        F.initcap(clean_str("location")).alias("location"),
        clean_str("manager_name").alias("manager_name"),
        F.col("_opened").alias("opened_date"),
        F.when(region_c.isin("North", "South", "East", "West", "Central"), region_c).alias("region"),
        F.when(type_c.isin("Urban", "Rural", "Semi-Urban", "Metro"), type_c).alias("branch_type"),
        contact.alias("contact_number"),
        F.col("_opened_format"), F.col("_opened_ambiguous"),
        (F.col("region") != region_c).alias("_region_fixed"), (F.col("branch_type") != type_c).alias("_type_fixed"),
        contact_ok.alias("_contact_ok"), contact_changed.alias("_contact_changed"), F.col("contact_number").alias("_contact_raw"),
        "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num"))
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

# COMMAND ----------

c = bronze("customers")
c = with_parsed_date(c, "dob", "_dob")
c = with_parsed_date(c, "registration_date", "_reg")
phone, phone_ok, phone_changed = norm_indian_phone("phone")

e0 = F.lower(clean_str("email"))
e1 = F.regexp_replace(e0, "@+", "@")
dom_rx = "(" + "|".join(re.escape(d) for d in EMAIL_DOMAINS) + ")$"
e2 = F.when(~e1.contains("@") & e1.rlike(dom_rx), F.regexp_replace(e1, dom_rx, "@$1")).otherwise(e1)
g = F.lower(clean_str("gender"))
gender = (F.when(g.isin("m", "male"), "Male").when(g.isin("f", "female"), "Female")
           .when(g.isin("o", "other"), "Other").otherwise("Unknown"))
k = F.lower(clean_str("kyc_status"))
kyc = F.when(k.isin("verified", "pending", "rejected"), F.initcap(k)).otherwise("Unknown")
addr = clean_str("address")
addr = F.regexp_replace(F.when(addr == F.lower(addr), F.initcap(addr)).otherwise(addr), r"\bMg\b", "MG")

c = (c.select(
        F.upper(clean_str("customer_id")).alias("customer_id"),
        clean_str("customer_name").alias("customer_name"),
        F.when(e2.rlike(EMAIL_RE), e2).alias("email"),
        phone.alias("phone"),
        F.upper(clean_str("account_id")).alias("account_id"),
        gender.alias("gender"),
        F.when((F.col("_dob") <= BD) & (F.col("_dob") >= F.add_months(BD, -1200)), F.col("_dob")).alias("dob"),
        addr.alias("address"),
        F.initcap(F.trim(F.substring_index(addr, ",", -1))).alias("city"),
        kyc.alias("kyc_status"),
        F.col("_reg").alias("registration_date"),
        F.upper(clean_str("transaction_id")).alias("ref_transaction_id"),
        e0.alias("_email_in"), e2.alias("_email_fixed"), phone_ok.alias("_phone_ok"), phone_changed.alias("_phone_changed"),
        F.col("phone").alias("_phone_raw"), clean_str("gender").alias("_gender_raw"), clean_str("kyc_status").alias("_kyc_raw"),
        F.col("_dob").alias("_dob_parsed"), F.col("dob").alias("_dob_raw"),
        "_dob_format", "_dob_ambiguous", "_reg_format", "_reg_ambiguous",
        "_raw", "_batch_id", "_batch_seq", "_source_file", "_row_num"))
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
num = lambda colname: F.when(F.regexp_extract(F.col(colname), r"(\d+)", 1) != "", F.regexp_extract(F.col(colname), r"(\d+)", 1).cast("int"))
batch_side = kept.select("customer_id", "account_id", "registration_date", F.lit(False).alias("_existing"))
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
people = (kept.select("customer_id", "customer_name", "dob")
          .unionByName(spark.table(f"{GOLD}.dim_customer").filter("is_current")
                       .join(kept.select("customer_id"), "customer_id", "left_anti").select("customer_id", "customer_name", "dob"))
          .filter(F.col("dob").isNotNull())
          .withColumn("_pk", F.concat_ws("|", F.lower("customer_name"), F.col("dob").cast("string"))))
people = (people.withColumn("_ids", F.collect_set("customer_id").over(W.partitionBy("_pk")))
                .withColumn("potential_duplicate_of", F.array_min(F.filter("_ids", lambda x: x != F.col("customer_id"))))
                .select("customer_id", "potential_duplicate_of"))
kept = (kept.join(people, "customer_id", "left")
            .withColumn("dq_flags", F.when(F.col("potential_duplicate_of").isNotNull(),
                                           F.array_union("dq_flags", F.array(F.lit("POTENTIAL_DUPLICATE_PERSON")))).otherwise(F.col("dq_flags"))))
finish("customers", "customer_id", kept, rejected, CUSTOMER_COLS + ["potential_duplicate_of"])

# COMMAND ----------

# MAGIC %md ## Products

# COMMAND ----------

p = bronze("products")
p = with_parsed_date(p, "launch_date", "_launch")
price_txt = F.regexp_replace(clean_str("price"), r"(?i)rs\.?|inr|,|\s", "")
p = p.withColumn("_price_txt", price_txt).withColumn("_price", tcast("_price_txt", "DECIMAL(18,2)"))
a = F.lower(clean_str("is_active"))
active = F.when(a.isin("true", "1", "yes", "y", "t"), True).when(a.isin("false", "0", "no", "n", "f"), False).otherwise(False)
cat_u = F.upper(clean_str("category"))
category = F.when(cat_u == "SME", "SME").otherwise(F.initcap(F.lower(clean_str("category"))))
ptype = F.initcap(F.lower(clean_str("product_type")))
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
kept, rejected = split_and_dedupe(p, "product_id", PRODUCT_COLS)
finish("products", "product_id", kept, rejected, PRODUCT_COLS)
