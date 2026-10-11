# Databricks notebook source
# MAGIC %md
# MAGIC # Common configuration & helpers
# MAGIC Shared by every pipeline notebook via `%run ./includes/common`.
# MAGIC
# MAGIC * **Parameters** (job / widget): `catalog`, `batch_id`, optional `business_date`, `source_folder`
# MAGIC * **Layers** (Unity Catalog schemas): `rb_landing` (Volume, raw files) → `rb_bronze` → `rb_silver` → `rb_gold`; `rb_audit` for pipeline metadata
# MAGIC * Written for **serverless** compute: no RDD / sparkContext / cache, ANSI-safe parsing (`try_*`)

# COMMAND ----------

import json, re, hashlib, os, shutil
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql import functions as F, Window as W, DataFrame
from pyspark.sql.types import StringType, StructType, StructField
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_id", "day1", "Batch id")
dbutils.widgets.text("business_date", "", "Business date override (yyyy-MM-dd)")
dbutils.widgets.text("source_folder", "", "Landing folder override")

CATALOG = dbutils.widgets.get("catalog").strip() or "workspace"
BATCH_ID = dbutils.widgets.get("batch_id").strip()

LANDING, BRONZE, SILVER, GOLD, AUDIT = (f"{CATALOG}.rb_{s}" for s in ("landing", "bronze", "silver", "gold", "audit"))
RAW_VOLUME_PATH = f"/Volumes/{CATALOG}/rb_landing/raw"
ARCHIVE_VOLUME_PATH = f"/Volumes/{CATALOG}/rb_landing/archive"

# Known batches. A new drop (e.g. day3) can be run without code changes by passing
# source_folder + business_date as job parameters.
BATCH_REGISTRY = {
    "day1": {"seq": 1, "folder": "RetailBank_SourceData", "business_date": "2026-09-30"},
    "day2": {"seq": 2, "folder": "RetailBank_SourceData_Day2", "business_date": "2026-10-01"},
}

# Source files are matched by pattern so Day 1 / Day 2 naming differences need no config.
ENTITY_SOURCES = {
    "branches":     {"pattern": r"^branches.*\.csv$",            "format": "csv"},
    "customers":    {"pattern": r"^customer.*\.csv$",            "format": "csv"},
    "products":     {"pattern": r"^products.*\.(json|txt)$",     "format": "json_array"},
    "transactions": {"pattern": r"^transactions.*\.csv$",        "format": "csv"},
}

# Business columns per entity, in source order. Bronze keeps them all as STRING.
SOURCE_COLUMNS = {
    "branches": ["branch_id", "branch_name", "location", "manager_name", "opened_date", "region", "branch_type", "contact_number"],
    "customers": ["customer_id", "customer_name", "email", "phone", "account_id", "gender", "dob", "address", "kyc_status", "registration_date", "transaction_id"],
    "products": ["product_id", "product_name", "product_type", "price", "launch_date", "is_active", "category", "vendor_name"],
    "transactions": ["transaction_id", "account_id", "product_id", "branch_id", "date", "timestamp", "amount", "payment_method", "status", "currency", "remarks", "is_refund"],
}

def raw_schema(entity):
    return StructType([StructField(c, StringType()) for c in SOURCE_COLUMNS[entity]])

# COMMAND ----------

def _batch_control_row(batch_id):
    if not spark.catalog.tableExists(f"{AUDIT}.batch_control"):
        return None
    rows = spark.table(f"{AUDIT}.batch_control").filter(F.col("batch_id") == batch_id).collect()
    return rows[0] if rows else None

def resolve_batch(batch_id):
    """Batch metadata from widgets > registry > batch_control table."""
    reg = BATCH_REGISTRY.get(batch_id, {})
    ctl = _batch_control_row(batch_id)
    folder = dbutils.widgets.get("source_folder").strip() or reg.get("folder") or (ctl and ctl["source_folder"])
    bdate = dbutils.widgets.get("business_date").strip() or reg.get("business_date") or (ctl and str(ctl["business_date"]))
    if not folder or not bdate:
        raise ValueError(f"Unknown batch '{batch_id}': pass source_folder and business_date parameters")
    seq = reg.get("seq") or (ctl and ctl["batch_seq"])
    if not seq:
        mx = spark.sql(f"SELECT coalesce(max(batch_seq), 0) m FROM {AUDIT}.batch_control").first()["m"]
        seq = mx + 1
    return {"batch_id": batch_id, "seq": int(seq), "folder": folder, "business_date": bdate}

BATCH = resolve_batch(BATCH_ID)
BUSINESS_DATE = BATCH["business_date"]
BATCH_SEQ = BATCH["seq"]
print(f"catalog={CATALOG} batch={BATCH}")

# COMMAND ----------

# ---------- generic column helpers ----------

def clean_str(c):
    """trim, collapse internal whitespace, empty -> NULL"""
    col = c if not isinstance(c, str) else F.col(c)
    v = F.regexp_replace(F.trim(col), r"\s+", " ")
    return F.when(v == "", F.lit(None)).otherwise(v)

def codes(*pairs):
    """Build an array<string> of codes for which the condition is TRUE (NULL-safe)."""
    return F.filter(F.array(*[F.when(cond, F.lit(code)) for cond, code in pairs]), lambda x: x.isNotNull())

def row_hash(*cols):
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in cols]), 256)

def tcast(colname, typ):
    return F.expr(f"try_cast(`{colname}` AS {typ})")

def _safe_make_date(y, m, d):
    """make_date that returns NULL instead of failing for impossible dates (ANSI-safe)."""
    ok = m.between(1, 12) & (d >= 1) & y.between(1800, 2200)
    return F.when(ok, F.when(d <= F.dayofmonth(F.last_day(F.make_date(y, m, F.lit(1)))), F.make_date(y, m, d)))

def date_candidates(colname):
    """
    Returns (ymd, dmy, mdy, shape) columns for a raw date string.
    Supported shapes: yyyy-MM-dd, yyyy/MM/dd, dd-MM-yyyy, dd/MM/yyyy, MM/dd/yyyy (+ optional time part).
    """
    s = F.trim(F.split(F.trim(F.col(colname)), " ").getItem(0))
    ymd_re, xxy_re = r"^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$", r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$"
    is_ymd, is_xxy = s.rlike(ymd_re), s.rlike(xxy_re)
    gi = lambda rx, i: F.regexp_extract(s, rx, i).cast("int")
    ymd = F.when(is_ymd, _safe_make_date(gi(ymd_re, 1), gi(ymd_re, 2), gi(ymd_re, 3)))
    a, b, y = gi(xxy_re, 1), gi(xxy_re, 2), gi(xxy_re, 3)
    dmy = F.when(is_xxy, _safe_make_date(y, b, a))
    mdy = F.when(is_xxy, _safe_make_date(y, a, b))
    shape = (F.when(s.rlike(r"^\d{4}-\d{1,2}-\d{1,2}$"), "yyyy-MM-dd")
              .when(s.rlike(r"^\d{4}/\d{1,2}/\d{1,2}$"), "yyyy/MM/dd")
              .when(s.rlike(r"^\d{1,2}-\d{1,2}-\d{4}$"), "dd-MM-yyyy")
              .when(s.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), "xx/xx/yyyy")
              .when(s.isNull(), None).otherwise("UNPARSEABLE"))
    return ymd, dmy, mdy, shape

def with_parsed_date(df, colname, out):
    """
    Adds <out> (DATE), <out>_format and <out>_ambiguous.
    For dd/MM vs MM/dd: day>12 or month>12 disambiguates; otherwise Indian convention dd/MM/yyyy is assumed
    and the value is flagged ambiguous. dd-MM-yyyy (dash) is always day-first.
    """
    ymd, dmy, mdy, shape = date_candidates(colname)
    s = F.trim(F.col(colname))
    first = F.regexp_extract(s, r"^(\d{1,2})[-/]", 1).cast("int")
    second = F.regexp_extract(s, r"^\d{1,2}[-/](\d{1,2})[-/]", 1).cast("int")
    resolved = (F.when(ymd.isNotNull(), ymd)
                 .when(shape == "dd-MM-yyyy", dmy)
                 .when(first > 12, dmy)
                 .when(second > 12, mdy)
                 .otherwise(dmy))
    fmt = (F.when(shape == "xx/xx/yyyy", F.when(first > 12, "dd/MM/yyyy").when(second > 12, "MM/dd/yyyy").otherwise("dd/MM/yyyy (assumed)"))
            .otherwise(shape))
    amb = (shape == "xx/xx/yyyy") & (first <= 12) & (second <= 12) & (first != second)
    return (df.withColumn(out, resolved).withColumn(f"{out}_format", fmt)
              .withColumn(f"{out}_ambiguous", F.coalesce(amb, F.lit(False))))

def norm_indian_phone(colname):
    """Returns (normalized '+91-XXXXXXXXXX' or NULL, is_valid, was_changed)."""
    raw = F.trim(F.col(colname))
    digits = F.regexp_replace(raw, r"\D", "")
    with_cc = (F.length(digits) == 12) & digits.startswith("91")
    # '+..' means a country code is present: it must be +91 followed by exactly 10 digits
    # (so '+91-84858333' is invalid, not re-read as the 10-digit number 9184858333)
    ten = (F.when(raw.startswith("+"), F.when(with_cc, F.substring(digits, 3, 10)))
            .when(F.length(digits) == 10, digits)
            .when(with_cc, F.substring(digits, 3, 10)))
    val = F.when(ten.isNotNull(), F.concat(F.lit("+91-"), ten))
    return val, val.isNotNull(), val.isNotNull() & (val != raw)

# COMMAND ----------

# ---------- table / audit helpers ----------

def table_exists(name):
    return spark.catalog.tableExists(name)

def overwrite_partition(df, table, col="_batch_id", value=None):
    """Idempotent write: replace only this batch's slice of the table."""
    value = value or BATCH_ID
    w = df.write.format("delta").option("mergeSchema", "true")
    if table_exists(table):
        w.mode("overwrite").option("replaceWhere", f"{col} = '{value}'").saveAsTable(table)
    else:
        w.mode("overwrite").saveAsTable(table)

def write_metrics(entity, metrics: dict, stage):
    """Key/value load statistics per batch (drives KPI 11 and the run summary)."""
    rows = [(BATCH_ID, BATCH_SEQ, stage, entity, k, int(v)) for k, v in metrics.items()]
    df = (spark.createDataFrame(rows, "batch_id string, batch_seq int, stage string, entity string, metric string, value long")
          .withColumn("recorded_at", F.current_timestamp()))
    spark.sql(f"DELETE FROM {AUDIT}.load_stats WHERE batch_id = '{BATCH_ID}' AND entity = '{entity}' AND stage = '{stage}'")
    df.write.mode("append").saveAsTable(f"{AUDIT}.load_stats")
    print(f"[{stage}] {entity}: {metrics}")

INFO_REASONS = ["DUPLICATE_EXACT", "DUPLICATE_SUPERSEDED"]

def write_quarantine(rejects: DataFrame, entity, stage):
    """
    rejects must carry: _batch_id, _source_file, _row_num, record_key, reason_codes (array<string>), _raw (struct)
    Every rejected record keeps its untouched raw payload + all reason codes -> nothing is silently dropped.
    """
    out = (rejects
        .select(
            F.col("_batch_id").alias("batch_id"),
            F.lit(BATCH_SEQ).alias("batch_seq"),
            F.lit(entity).alias("entity"),
            F.lit(stage).alias("stage"),
            F.col("_source_file").alias("source_file"),
            F.col("_row_num").cast("long").alias("source_row_num"),
            F.col("record_key"),
            F.col("reason_codes")[0].alias("reason_code"),
            F.col("reason_codes"),
            F.when(F.col("reason_codes")[0].isin(INFO_REASONS), "INFO").otherwise("ERROR").alias("severity"),
            F.to_json(F.col("_raw")).alias("raw_record"),
            F.current_timestamp().alias("quarantined_at"),
            F.lit(None).cast("string").alias("resolved_batch_id"),
            F.lit(None).cast("string").alias("resolution")))
    out.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{SILVER}.quarantine")
    return out

def reset_quarantine(entity, stage):
    """Make a re-run of the same batch idempotent."""
    if table_exists(f"{SILVER}.quarantine"):
        spark.sql(f"""DELETE FROM {SILVER}.quarantine
                      WHERE batch_id = '{BATCH_ID}' AND entity = '{entity}' AND stage = '{stage}'""")

# COMMAND ----------

# ---------- SCD Type 2 upsert (atomic single MERGE) ----------

SCD_EFFECTIVE_START = "1900-01-01 00:00:00"
SCD_EFFECTIVE_END = "9999-12-31 23:59:59"

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
    cur = spark.table(target).filter("is_current")
    s = src.alias("s").join(cur.alias("c"), F.col(f"s.{key}") == F.col(f"c.{key}"), "left")
    sel = [F.col(f"s.{key}").alias(key)]
    for c in attrs:
        sel.append(F.coalesce(F.col(f"s.{c}"), F.col(f"c.{c}")).alias(c))
    sel += [F.col(f"c.{key}").isNotNull().alias("_exists"), F.col("c.hash_scd2").alias("_cur_h2"),
            F.col("c.hash_type1").alias("_cur_h1"), F.col("c.version").alias("_cur_version")]
    staged = (s.select(*sel)
              .withColumn("hash_scd2", row_hash(*scd2_cols))
              .withColumn("hash_type1", row_hash(*type1_cols))
              .withColumn("_change_type",
                  F.when(~F.col("_exists"), "NEW")
                   .when(F.col("hash_scd2") != F.col("_cur_h2"), "SCD2_CHANGE")
                   .when(F.col("hash_type1") != F.col("_cur_h1"), "TYPE1_CHANGE")
                   .otherwise("UNCHANGED")))

    counts = {r["_change_type"]: r["n"] for r in staged.groupBy("_change_type").agg(F.count("*").alias("n")).collect()}
    changes = staged.filter("_change_type <> 'UNCHANGED'")
    merge_src = (changes.withColumn("_merge_key", F.col(key))
                 .unionByName(changes.filter("_change_type = 'SCD2_CHANGE'").withColumn("_merge_key", F.lit(None).cast("string"))))

    eff_ts = f"CAST('{BUSINESS_DATE} 00:00:00' AS TIMESTAMP)"
    insert_vals = {key: f"s.{key}", **{c: f"s.{c}" for c in attrs},
        "hash_scd2": "s.hash_scd2", "hash_type1": "s.hash_type1",
        "version": "coalesce(s._cur_version, 0) + 1",
        "effective_from": f"CASE WHEN s._change_type = 'NEW' THEN CAST('{SCD_EFFECTIVE_START}' AS TIMESTAMP) ELSE {eff_ts} END",
        "effective_to": f"CAST('{SCD_EFFECTIVE_END}' AS TIMESTAMP)",
        "is_current": "true",
        "created_batch_id": f"'{BATCH_ID}'", "updated_batch_id": f"'{BATCH_ID}'",
        "created_at": "current_timestamp()", "updated_at": "current_timestamp()"}
    (DeltaTable.forName(spark, target).alias("t")
        .merge(merge_src.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(condition="s._change_type = 'SCD2_CHANGE'", set={
            "is_current": "false", "effective_to": eff_ts,
            "updated_batch_id": f"'{BATCH_ID}'", "updated_at": "current_timestamp()"})
        .whenMatchedUpdate(condition="s._change_type = 'TYPE1_CHANGE'", set={
            **{c: f"s.{c}" for c in type1_cols}, "hash_type1": "s.hash_type1",
            "updated_batch_id": f"'{BATCH_ID}'", "updated_at": "current_timestamp()"})
        .whenNotMatchedInsert(condition="s._change_type IN ('NEW', 'SCD2_CHANGE')", values=insert_vals)
        .execute())
    return {"received": sum(counts.values()), "new": counts.get("NEW", 0), "scd2_changed": counts.get("SCD2_CHANGE", 0),
            "type1_updated": counts.get("TYPE1_CHANGE", 0), "unchanged": counts.get("UNCHANGED", 0)}
