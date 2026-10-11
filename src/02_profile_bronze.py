# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Profile Bronze
# MAGIC Column-level profile of this batch for every entity → `rb_audit.data_profile`.
# MAGIC `top_patterns` replaces digits with `9` and letters with `a`/`A`, which exposes mixed date formats,
# MAGIC phone formats, `Rs.` / comma amounts, `@@` emails etc. These findings drive the silver cleansing rules.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

def profile(entity):
    df = spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID)
    cols = SOURCE_COLUMNS[entity]
    long = df.select(F.explode(F.create_map(*[x for c in cols for x in (F.lit(c), F.col(c))])).alias("column_name", "v"))
    long = long.withColumn("pattern", F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("v"), r"\d", "9"), "[a-z]", "a"), "[A-Z]", "A"))
    base = long.groupBy("column_name").agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("v").isNull() | (F.trim("v") == ""), 1).otherwise(0)).alias("null_or_blank"),
        F.countDistinct("v").alias("distinct_values"),
        F.sum(F.when(F.col("v") != F.trim("v"), 1).otherwise(0)).alias("has_leading_trailing_space"),
        F.min(F.length("v")).alias("min_length"), F.max(F.length("v")).alias("max_length"))
    def topn(colname, alias, n=6):
        w = W.partitionBy("column_name").orderBy(F.desc("n"), colname)
        return (long.groupBy("column_name", colname).agg(F.count("*").alias("n"))
                .withColumn("r", F.row_number().over(w)).filter(F.col("r") <= n)
                .groupBy("column_name").agg(F.concat_ws(" | ", F.collect_list(
                    F.concat(F.coalesce(F.col(colname), F.lit("<null>")), F.lit(" ("), F.col("n"), F.lit(")")))).alias(alias)))
    return (base.join(topn("v", "top_values"), "column_name").join(topn("pattern", "top_patterns"), "column_name")
            .withColumn("batch_id", F.lit(BATCH_ID)).withColumn("entity", F.lit(entity))
            .withColumn("profiled_at", F.current_timestamp()))

prof = reduce(DataFrame.unionByName, [profile(e) for e in ENTITY_SOURCES])
prof = prof.select(*spark.table(f"{AUDIT}.data_profile").columns)
spark.sql(f"DELETE FROM {AUDIT}.data_profile WHERE batch_id = '{BATCH_ID}'")
prof.write.mode("append").saveAsTable(f"{AUDIT}.data_profile")
display(spark.table(f"{AUDIT}.data_profile").filter(F.col("batch_id") == BATCH_ID).orderBy("entity", "column_name"))

# COMMAND ----------

# MAGIC %md ### Key-level profile (duplicates)

# COMMAND ----------

keys = {"branches": "branch_id", "customers": "customer_id", "products": "product_id", "transactions": "transaction_id"}
for e, k in keys.items():
    df = spark.table(f"{BRONZE}.{e}").filter(F.col("_batch_id") == BATCH_ID)
    cols = SOURCE_COLUMNS[e]
    r = df.agg(F.count("*").alias("rows"), F.countDistinct(k).alias("distinct_keys")).first().asDict()
    r["distinct_rows"] = df.select(*cols).distinct().count()
    print(f"{e:13s} rows={r['rows']:5d} distinct_keys={r['distinct_keys']:5d} exact_dup_rows={r['rows'] - r['distinct_rows']:4d} "
          f"dup_key_rows={r['rows'] - r['distinct_keys']:4d}")
