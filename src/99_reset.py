# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset (demo only)
# MAGIC Drops every table in bronze / silver / gold / audit and empties the archive Volume so the end-to-end demo
# MAGIC (Day 1 → Day 2) can be replayed from scratch. **The landing Volume (`rb_landing.raw`) is never touched.**
# MAGIC Requires `confirm = YES`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("confirm", "NO")
catalog = dbutils.widgets.get("catalog")
assert dbutils.widgets.get("confirm") == "YES", "Set confirm=YES to reset the pipeline"

import shutil, os
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

archive = f"/Volumes/{catalog}/rb_landing/archive"
if os.path.exists(archive):
    for d in os.listdir(archive):
        shutil.rmtree(f"{archive}/{d}", ignore_errors=True)
print("reset complete")
