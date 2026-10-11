# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Profile Bronze
# MAGIC Column-level profile of this batch for every entity → `rb_audit.data_profile`.
# MAGIC `top_patterns` replaces digits with `9` and letters with `a`/`A`, which exposes mixed date formats,
# MAGIC phone formats, `Rs.` / comma amounts, `@@` emails etc. These findings drive the silver cleansing rules.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC The brief says *"profile each file before writing cleansing rules"*. This notebook is that evidence.
# MAGIC For every column of every file it records: how many values are blank, how many distinct values exist,
# MAGIC how many have stray spaces, the most frequent values and the most frequent **shapes** (patterns).
# MAGIC
# MAGIC The pattern trick is the key insight: `2026-05-23` becomes `9999-99-99` and `04/08/2026` becomes
# MAGIC `99/99/9999`, so one look at `top_patterns` shows that the date column uses 4 formats, the amount column
# MAGIC has `Aa.99999` (Rs. prefix) and `999,999` (commas), and phones come in 6 layouts.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

def profile(entity):
    # Only profile the rows of the current batch.
    df = spark.table(f"{BRONZE}.{entity}").filter(F.col("_batch_id") == BATCH_ID)
    cols = SOURCE_COLUMNS[entity]
    # "Unpivot" the table into two columns (column_name, v) so ONE aggregation profiles ALL columns at once:
    #   branch_id | region          ->   column_name | v
    #   B018      | South                branch_id   | B018
    #                                    region      | South
    long = df.select(F.explode(F.create_map(*[x for c in cols for x in (F.lit(c), F.col(c))])).alias("column_name", "v"))
    # Pattern = the value with every digit replaced by 9, lowercase letters by a, uppercase by A.
    long = long.withColumn("pattern", F.regexp_replace(F.regexp_replace(F.regexp_replace(F.col("v"), r"\d", "9"), "[a-z]", "a"), "[A-Z]", "A"))
    # Basic statistics per column.
    base = long.groupBy("column_name").agg(
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("v").isNull() | (F.trim("v") == ""), 1).otherwise(0)).alias("null_or_blank"),
        F.countDistinct("v").alias("distinct_values"),
        F.sum(F.when(F.col("v") != F.trim("v"), 1).otherwise(0)).alias("has_leading_trailing_space"),
        F.min(F.length("v")).alias("min_length"), F.max(F.length("v")).alias("max_length"))
    def topn(colname, alias, n=6):
        # The n most frequent values (or patterns) per column, as one readable string:
        #   "INR (909) | EUR (12) | inr (10) | USD (9)"
        w = W.partitionBy("column_name").orderBy(F.desc("n"), colname)
        return (long.groupBy("column_name", colname).agg(F.count("*").alias("n"))
                .withColumn("r", F.row_number().over(w)).filter(F.col("r") <= n)
                .groupBy("column_name").agg(F.concat_ws(" | ", F.collect_list(
                    F.concat(F.coalesce(F.col(colname), F.lit("<null>")), F.lit(" ("), F.col("n"), F.lit(")")))).alias(alias)))
    return (base.join(topn("v", "top_values"), "column_name").join(topn("pattern", "top_patterns"), "column_name")
            .withColumn("batch_id", F.lit(BATCH_ID)).withColumn("entity", F.lit(entity))
            .withColumn("profiled_at", F.current_timestamp()))

# Profile all four entities, put the columns in the order of the audit table, and save (re-run safe).
prof = reduce(DataFrame.unionByName, [profile(e) for e in ENTITY_SOURCES])
prof = prof.select(*spark.table(f"{AUDIT}.data_profile").columns)
spark.sql(f"DELETE FROM {AUDIT}.data_profile WHERE batch_id = '{BATCH_ID}'")
prof.write.mode("append").saveAsTable(f"{AUDIT}.data_profile")
display(spark.table(f"{AUDIT}.data_profile").filter(F.col("batch_id") == BATCH_ID).orderBy("entity", "column_name"))

# COMMAND ----------

# MAGIC %md ### Key-level profile (duplicates)

# COMMAND ----------

# Duplicate check per entity (printed in the task output):
#   exact_dup_rows = rows that are 100% identical to another row (re-sent records)
#   dup_key_rows   = rows whose ID appears more than once (includes "same ID, different content" conflicts)
keys = {"branches": "branch_id", "customers": "customer_id", "products": "product_id", "transactions": "transaction_id"}
for e, k in keys.items():
    df = spark.table(f"{BRONZE}.{e}").filter(F.col("_batch_id") == BATCH_ID)
    cols = SOURCE_COLUMNS[e]
    r = df.agg(F.count("*").alias("rows"), F.countDistinct(k).alias("distinct_keys")).first().asDict()
    r["distinct_rows"] = df.select(*cols).distinct().count()
    print(f"{e:13s} rows={r['rows']:5d} distinct_keys={r['distinct_keys']:5d} exact_dup_rows={r['rows'] - r['distinct_rows']:4d} "
          f"dup_key_rows={r['rows'] - r['distinct_keys']:4d}")
