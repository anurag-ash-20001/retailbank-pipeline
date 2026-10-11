# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset (demo only)
# MAGIC Drops every table in bronze / silver / gold / audit and empties the archive Volume so the end-to-end demo
# MAGIC (Day 1 → Day 2) can be replayed from scratch. **The landing Volume (`rb_landing.raw`) is never touched.**
# MAGIC Requires `confirm = YES`.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC It is only used by the *End-to-End Demo* job so the whole story (empty → Day 1 → Day 2) can be shown again.
# MAGIC The source files stay in the landing Volume, so after a reset the pipeline rebuilds exactly the same results.
# MAGIC The `confirm` parameter is a safety switch: running it by accident without `confirm=YES` stops immediately.

# COMMAND ----------

# Parameters: which catalog to clean, and the safety switch.
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("confirm", "NO")
catalog = dbutils.widgets.get("catalog")
assert dbutils.widgets.get("confirm") == "YES", "Set confirm=YES to reset the pipeline"

import shutil, os
# Drop every table and view of the four pipeline schemas. Views must be dropped with DROP VIEW,
# so the object type is checked first. (The schemas themselves are kept; 00_setup recreates the tables.)
for schema in ("rb_bronze", "rb_silver", "rb_gold", "rb_audit"):
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect():
        if row["isTemporary"]:
            continue
        name = f"{catalog}.{schema}.{row['tableName']}"
        kind = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").filter("col_name = 'Type'").first()
        if kind and kind["data_type"] == "VIEW":
            spark.sql(f"DROP VIEW IF EXISTS {name}")
        else:
            spark.sql(f"DROP TABLE IF EXISTS {name}")
        print("dropped", name)

# Empty the archive Volume (one folder per batch). The raw landing files are NOT deleted.
archive = f"/Volumes/{catalog}/rb_landing/archive"
if os.path.exists(archive):
    for d in os.listdir(archive):
        shutil.rmtree(f"{archive}/{d}", ignore_errors=True)
print("reset complete")
