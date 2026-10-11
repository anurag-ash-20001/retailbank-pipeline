# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest → Bronze
# MAGIC 1. Locate this batch's files in the landing Volume (`/Volumes/<catalog>/rb_landing/raw/<folder>`)
# MAGIC 2. Copy each file **unchanged** into the immutable archive Volume (`archive/<batch_id>/`) – never overwritten
# MAGIC 3. Load every record as STRING (no type coercion, whitespace preserved) into `rb_bronze.<entity>` with
# MAGIC    metadata `_batch_id, _source_file, _row_num, _ingested_at`
# MAGIC 4. `products` JSON: validate the document; if structurally broken, salvage object-by-object and log the defect
# MAGIC
# MAGIC Idempotent: re-running a batch replaces only that batch's slice (`replaceWhere _batch_id`).

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

landing_dir = f"{RAW_VOLUME_PATH}/{BATCH['folder']}"
archive_dir = f"{ARCHIVE_VOLUME_PATH}/{BATCH_ID}"
os.makedirs(archive_dir, exist_ok=True)

spark.sql(f"DELETE FROM {AUDIT}.batch_control WHERE batch_id = '{BATCH_ID}'")
spark.sql(f"""INSERT INTO {AUDIT}.batch_control VALUES
  ('{BATCH_ID}', {BATCH_SEQ}, '{BATCH['folder']}', DATE'{BUSINESS_DATE}', 'RUNNING', current_timestamp(), NULL, NULL)""")

files = sorted(os.listdir(landing_dir))
print("Landing files:", files)

def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def archive(fname):
    """Immutable copy. If the archive already has the file, it must be byte-identical."""
    src, dst = f"{landing_dir}/{fname}", f"{archive_dir}/{fname}"
    if os.path.exists(dst):
        if md5(dst) != md5(src):
            raise RuntimeError(f"{fname}: landing file differs from archived copy for batch {BATCH_ID}. "
                               "Batches are immutable - land the new drop as a new batch id.")
    else:
        shutil.copyfile(src, dst)
    return dst

# COMMAND ----------

def meta(df, fname):
    return (df.withColumn("_batch_id", F.lit(BATCH_ID))
              .withColumn("_batch_seq", F.lit(BATCH_SEQ))
              .withColumn("_source_file", F.lit(fname))
              .withColumn("_ingested_at", F.current_timestamp()))

def read_csv_raw(path, entity):
    df = (spark.read.format("csv")
          .option("header", "true").option("inferSchema", "false")
          .option("ignoreLeadingWhiteSpace", "false").option("ignoreTrailingWhiteSpace", "false")
          .option("quote", '"').option("escape", '"').option("mode", "PERMISSIVE")
          .schema(raw_schema(entity))
          .load(path))
    # single small file -> single partition -> monotonically_increasing_id preserves file order
    return df.withColumn("_row_num", F.monotonically_increasing_id() + 2)   # +2 = 1-based line no. incl. header

def read_json_array_raw(path):
    """
    Parses a JSON array file. On a structural defect (e.g. missing comma between objects, truncated or
    concatenated feeds) the document is salvaged by extracting each top-level {...} object independently.
    Returns (dataframe, structural_issue_text, unparseable_fragments)
    """
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    issue = None
    try:
        doc = json.loads(text)
        objs = doc if isinstance(doc, list) else [doc]
        raw_objs = [json.dumps(o) for o in objs]
    except json.JSONDecodeError as e:
        line_no = text.count("\n", 0, e.pos) + 1
        issue = f"JSON_STRUCTURAL_DEFECT: {e.msg} at line {line_no} col {e.colno} (char {e.pos}); salvaged object-by-object"
        raw_objs = re.findall(r"\{[^{}]*\}", text, flags=re.S)
    rows, bad = [], []
    for i, frag in enumerate(raw_objs, start=1):
        try:
            o = json.loads(frag)
        except json.JSONDecodeError:
            bad.append((i, frag))
            continue
        def s(k):
            # keep the source representation: strings as-is, booleans/numbers as JSON text, missing/null -> None
            if k not in o or o[k] is None:
                return None
            return o[k] if isinstance(o[k], str) else json.dumps(o[k])
        rows.append(tuple(s(c) for c in SOURCE_COLUMNS["products"]) + (i, json.dumps(o),
                    "missing" if "is_active" not in o else ("null" if o["is_active"] is None else type(o["is_active"]).__name__)))
    schema = raw_schema("products").add("_row_num", "long").add("_raw_json", "string").add("_is_active_source_type", "string")
    return spark.createDataFrame(rows, schema), issue, bad

# COMMAND ----------

log_rows = []
for entity, cfg in ENTITY_SOURCES.items():
    matched = [f for f in files if re.match(cfg["pattern"], f, flags=re.I)]
    if not matched:
        print(f"WARNING: no file for {entity} in {landing_dir}")
        continue
    dfs = []
    reset_quarantine(entity, "BRONZE")
    for fname in matched:
        path = archive(fname)
        issue = None
        if cfg["format"] == "csv":
            df = read_csv_raw(path, entity)
        else:
            df, issue, bad = read_json_array_raw(path)
            if bad:
                bad_df = spark.createDataFrame([(BATCH_ID, fname, n, frag) for n, frag in bad],
                                               "_batch_id string, _source_file string, _row_num long, frag string")
                write_quarantine(bad_df.select("_batch_id", "_source_file", "_row_num",
                                               F.lit(None).cast("string").alias("record_key"),
                                               F.array(F.lit("JSON_UNPARSEABLE_FRAGMENT")).alias("reason_codes"),
                                               F.struct(F.col("frag").alias("fragment")).alias("_raw")), entity, "BRONZE")
        df = meta(df, fname)
        n = df.count()
        dfs.append(df)
        log_rows.append((BATCH_ID, BATCH_SEQ, entity, fname, f"{landing_dir}/{fname}", path,
                         os.path.getsize(path), md5(path), n + (len(bad) if cfg["format"] != "csv" else 0), issue))
        print(f"{entity:13s} {fname:30s} rows={n} {issue or ''}")
    overwrite_partition(reduce(DataFrame.unionByName, dfs), f"{BRONZE}.{entity}")

# COMMAND ----------

spark.sql(f"DELETE FROM {AUDIT}.ingestion_log WHERE batch_id = '{BATCH_ID}'")
(spark.createDataFrame(log_rows, """batch_id string, batch_seq int, entity string, source_file string, landing_path string,
                                     archive_path string, file_size_bytes long, file_md5 string, row_count long, structural_issues string""")
      .withColumn("ingested_at", F.current_timestamp())
      .write.mode("append").saveAsTable(f"{AUDIT}.ingestion_log"))
display(spark.table(f"{AUDIT}.ingestion_log").filter(F.col("batch_id") == BATCH_ID))
