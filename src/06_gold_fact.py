# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Gold – transaction fact (MERGE with late-arriving corrections)
# MAGIC * **Grain:** one row per `transaction_id` (degenerate key). Surrogate keys to the dimension **version valid at `txn_ts`**.
# MAGIC * New id → insert (version 1).
# MAGIC * Existing id with a different payload (late correction) → previous version copied to `fact_transaction_history`,
# MAGIC   fact row **updated in place** (version+1, `is_corrected`), so KPIs never double count.
# MAGIC * Same payload → no-op, so re-running a batch changes nothing (idempotent).

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

FACT, HIST = f"{GOLD}.fact_transaction", f"{GOLD}.fact_transaction_history"
src = spark.table(f"{SILVER}.transactions").filter(F.col("_batch_id") == BATCH_ID)

def pit(dim, sk, natural_src, natural_dim):
    """point-in-time surrogate key lookup"""
    d = spark.table(f"{GOLD}.{dim}").select(F.col(sk), F.col(natural_dim).alias(f"_{dim}_nk"),
                                            F.col("effective_from").alias(f"_{dim}_from"), F.col("effective_to").alias(f"_{dim}_to"))
    return d, (F.col(natural_src) == F.col(f"_{dim}_nk")) & (F.col("txn_ts") >= F.col(f"_{dim}_from")) & (F.col("txn_ts") < F.col(f"_{dim}_to"))

for dim, sk, ns, nd in [("dim_customer", "customer_sk", "account_id", "account_id"),
                        ("dim_product", "product_sk", "product_id", "product_id"),
                        ("dim_branch", "branch_sk", "branch_id", "branch_id")]:
    d, cond = pit(dim, sk, ns, nd)
    src = src.join(d, cond, "left")

staged = src.select(
    "transaction_id", "customer_sk", "product_sk", "branch_sk", "account_id", "customer_id", "product_id", "branch_id",
    "txn_ts", "txn_date", F.hour("txn_ts").alias("txn_hour"),
    "amount", F.when(F.col("is_refund"), -F.col("amount")).otherwise(F.col("amount")).cast("decimal(18,2)").alias("signed_amount"),
    "currency", (F.col("currency") == "INR").alias("is_inr"), "payment_method", "status", "is_refund", "remarks",
    "source_date_raw", "source_date_parsed", "dq_flags", "record_hash", F.col("_source_batch_id").alias("source_batch_id"))

assert staged.groupBy("transaction_id").count().filter("count > 1").count() == 0, "staged fact is not unique per transaction_id"

# COMMAND ----------

fact = spark.table(FACT)
cmp = staged.alias("s").join(fact.alias("f"), "transaction_id", "left").select(
    "transaction_id", F.col("f.transaction_id").isNotNull().alias("_exists"),
    (F.col("f.record_hash") != F.col("s.record_hash")).alias("_changed"), F.col("s.source_batch_id"))
counts = cmp.agg(
    F.sum(F.when(~F.col("_exists"), 1).otherwise(0)).alias("inserted"),
    F.sum(F.when(~F.col("_exists") & (F.col("source_batch_id") != BATCH_ID), 1).otherwise(0)).alias("inserted_recovered_from_quarantine"),
    F.sum(F.when(F.col("_exists") & F.col("_changed"), 1).otherwise(0)).alias("corrected"),
    F.sum(F.when(F.col("_exists") & ~F.col("_changed"), 1).otherwise(0)).alias("unchanged")).first().asDict()

# 1) archive the version being replaced (MERGE on id+version => idempotent)
prev = (fact.alias("f").join(staged.alias("s"), "transaction_id")
        .filter(F.col("f.record_hash") != F.col("s.record_hash"))
        .select("f.*").withColumn("superseded_by_batch_id", F.lit(BATCH_ID)).withColumn("superseded_at", F.current_timestamp()))
(DeltaTable.forName(spark, HIST).alias("h")
    .merge(prev.alias("p"), "h.transaction_id = p.transaction_id AND h.version = p.version")
    .whenNotMatchedInsertAll().execute())

# 2) upsert the fact
biz = [c for c in staged.columns if c not in ("transaction_id", "source_batch_id")]
(DeltaTable.forName(spark, FACT).alias("t")
    .merge(staged.alias("s"), "t.transaction_id = s.transaction_id")
    .whenMatchedUpdate(condition="t.record_hash <> s.record_hash", set={
        **{c: f"s.{c}" for c in biz},
        "dq_flags": "array_union(s.dq_flags, array('LATE_CORRECTION'))",
        "version": "t.version + 1", "is_corrected": "true", "correction_count": "t.correction_count + 1",
        "last_batch_id": f"'{BATCH_ID}'", "updated_at": "current_timestamp()"})
    .whenNotMatchedInsert(values={
        **{c: f"s.{c}" for c in staged.columns},
        "version": "1", "is_corrected": "false", "correction_count": "0",
        "first_batch_id": f"'{BATCH_ID}'", "last_batch_id": f"'{BATCH_ID}'",
        "loaded_at": "current_timestamp()", "updated_at": "current_timestamp()"})
    .execute())

write_metrics("transactions", counts, stage="GOLD")

# COMMAND ----------

display(spark.sql(f"""
  SELECT h.transaction_id, h.status AS old_status, f.status AS new_status, h.amount AS old_amount, f.amount AS new_amount,
         f.version, h.superseded_by_batch_id
  FROM {HIST} h JOIN {FACT} f USING (transaction_id) ORDER BY transaction_id"""))
