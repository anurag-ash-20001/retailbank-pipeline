# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gold – SCD Type 2 dimension upserts
# MAGIC | Dimension | SCD2 (history kept) | Type 1 (overwrite in place) |
# MAGIC |---|---|---|
# MAGIC | `dim_customer` | kyc_status, address, city | name, email, phone, account, gender, dob, registration, flags |
# MAGIC | `dim_branch` | manager_name, region, branch_type | name, location, opened_date, contact, flags |
# MAGIC | `dim_product` | price, is_active, category, product_type | name, launch_date, vendor, flags |
# MAGIC
# MAGIC A single atomic `MERGE` per dimension (see `scd2_upsert` in `includes/common`). Re-running the same batch is a no-op.
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC The brief asks to *"preserve history (SCD Type 2) where an attribute change is business-meaningful (e.g. KYC status, address)"*.
# MAGIC So for every dimension we decide which columns are **business-meaningful** (SCD2 → keep the old version)
# MAGIC and which are just **corrections** (Type 1 → overwrite). Examples from Day 2:
# MAGIC * C064 KYC `Pending → Verified`  → version 1 closed, version 2 created (KPI 12 reads this history)
# MAGIC * B069 manager `Manager_69 → Manager_69 (New)` → new branch version
# MAGIC * P024 price `11912 → 12412` → new product version
# MAGIC * a phone number re-formatted → updated in place, no new version
# MAGIC
# MAGIC This runs **before** the transactions are validated, so Day-2 transactions can already reference
# MAGIC the new customers, branch B901 and the new products.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# Configuration per dimension: target table, natural key, SCD2 columns, Type-1 columns.
# (dq_flags is Type 1: the list of fixes describes the latest source row, no history needed.)
DIMS = {
    "customers": dict(target=f"{GOLD}.dim_customer", key="customer_id",
                      scd2=["kyc_status", "address", "city"],
                      type1=["customer_name", "email", "phone", "account_id", "gender", "dob", "registration_date",
                             "ref_transaction_id", "potential_duplicate_of", "dq_flags"]),
    "branches": dict(target=f"{GOLD}.dim_branch", key="branch_id",
                     scd2=["manager_name", "region", "branch_type"],
                     type1=["branch_name", "location", "opened_date", "contact_number", "dq_flags"]),
    "products": dict(target=f"{GOLD}.dim_product", key="product_id",
                     scd2=["price", "is_active", "category", "product_type"],
                     type1=["product_name", "launch_date", "vendor_name", "dq_flags"]),
}

for entity, cfg in DIMS.items():
    # Only this batch's cleansed rows are merged (incremental: history is never re-processed).
    src = spark.table(f"{SILVER}.{entity}").filter(F.col("_batch_id") == BATCH_ID)
    # The generic helper classifies each row as NEW / SCD2_CHANGE / TYPE1_CHANGE / UNCHANGED and applies it.
    stats = scd2_upsert(cfg["target"], src, cfg["key"], cfg["scd2"], cfg["type1"])
    # e.g. Day 2 customers -> {received: 47, new: 22, scd2_changed: 14, type1_updated: 2, unchanged: 9}
    write_metrics(entity, stats, stage="GOLD")

# COMMAND ----------

# Quick summary for the task output: total rows, current rows, and expired (historical) versions per dimension.
for t in ("dim_customer", "dim_branch", "dim_product"):
    r = spark.sql(f"SELECT count(*) rows, sum(int(is_current)) current_rows, count(*) - sum(int(is_current)) history_rows FROM {GOLD}.{t}").first()
    print(f"{t:14s} rows={r['rows']} current={r['current_rows']} expired_versions={r['history_rows']}")

# Show the customers that have more than one version (= the SCD2 history created by Day 2).
display(spark.sql(f"""
  SELECT customer_id, version, kyc_status, address, effective_from, effective_to, is_current, created_batch_id
  FROM {GOLD}.dim_customer
  WHERE customer_id IN (SELECT customer_id FROM {GOLD}.dim_customer GROUP BY customer_id HAVING count(*) > 1)
  ORDER BY customer_id, version"""))
