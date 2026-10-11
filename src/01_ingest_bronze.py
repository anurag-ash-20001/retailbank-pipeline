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
# MAGIC
# MAGIC ### How to explain this notebook
# MAGIC **Bronze = "land the data exactly as received, plus who/when/where metadata".**
# MAGIC * No cleaning happens here on purpose. If a cleansing rule is wrong later, we can always re-process from
# MAGIC   bronze (or from the archived file) without asking the source system to send the data again.
# MAGIC * Every column is stored as text, so a value like `Rs.58345` or `04/08/2026` is kept exactly as it came.
# MAGIC * `_row_num` (the line number in the file) lets us point to the exact source line of any rejected record.
# MAGIC * The products file is broken JSON (a missing comma at line 41). A normal JSON reader would fail or drop
# MAGIC   data, so we detect the defect, log it, and still recover every product object individually.

# COMMAND ----------

# MAGIC %run ./includes/common

# COMMAND ----------

# Folder of this batch in the landing Volume, e.g. /Volumes/workspace/rb_landing/raw/RetailBank_SourceData_Day2
landing_dir = f"{RAW_VOLUME_PATH}/{BATCH['folder']}"
# Folder where the immutable copy of this batch's files is kept, e.g. .../rb_landing/archive/day2
archive_dir = f"{ARCHIVE_VOLUME_PATH}/{BATCH_ID}"
os.makedirs(archive_dir, exist_ok=True)

# Register the batch as RUNNING. (Delete first so that a re-run replaces the row instead of adding a 2nd one.)
# The last notebook (08_kpis) sets the status to SUCCEEDED, so a batch that failed half-way stays RUNNING.
spark.sql(f"DELETE FROM {AUDIT}.batch_control WHERE batch_id = '{BATCH_ID}'")
spark.sql(f"""INSERT INTO {AUDIT}.batch_control VALUES
  ('{BATCH_ID}', {BATCH_SEQ}, '{BATCH['folder']}', DATE'{BUSINESS_DATE}', 'RUNNING', current_timestamp(), NULL, NULL)""")

# List the files that landed for this batch (Volumes can be read with normal Python file functions).
files = sorted(os.listdir(landing_dir))
print("Landing files:", files)

def md5(path):
    # Checksum of the file content. Stored in the ingestion log and used to prove the archive copy is identical.
    # The file is read in 1 MB chunks so even a large file does not need to fit in memory.
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def archive(fname):
    """Immutable copy. If the archive already has the file, it must be byte-identical."""
    # Rules of the archive:
    #   * first run of a batch  -> copy the file into archive/<batch_id>/
    #   * re-run of the batch   -> the copy already exists; it is NOT overwritten
    #   * if someone changed the landing file after the batch was ingested, stop with an error:
    #     a new drop must be loaded as a new batch id (otherwise the audit trail would be lost).
    src, dst = f"{landing_dir}/{fname}", f"{archive_dir}/{fname}"
    if os.path.exists(dst):
        if md5(dst) != md5(src):
            raise RuntimeError(f"{fname}: landing file differs from archived copy for batch {BATCH_ID}. "
                               "Batches are immutable - land the new drop as a new batch id.")
    else:
        shutil.copyfile(src, dst)
    return dst          # we read from the ARCHIVED copy, so bronze always matches the archive

# COMMAND ----------

def meta(df, fname):
    # Ingestion metadata columns (underscore prefix = technical column, not a business column).
    return (df.withColumn("_batch_id", F.lit(BATCH_ID))          # which batch / day
              .withColumn("_batch_seq", F.lit(BATCH_SEQ))        # batch order number
              .withColumn("_source_file", F.lit(fname))          # which file
              .withColumn("_ingested_at", F.current_timestamp()))  # when it was loaded

def read_csv_raw(path, entity):
    # Read a CSV with an explicit all-STRING schema:
    #   header=true            -> skip the header line
    #   inferSchema=false      -> do NOT let Spark guess types (it would turn "Rs.58345" into NULL silently)
    #   ignore*WhiteSpace=false-> keep leading/trailing spaces (" West ", "Card ") so profiling can see them
    #   quote/escape '"'       -> addresses like "104 Linking Rd, Delhi" contain commas inside quotes
    #   mode=PERMISSIVE        -> a malformed line does not crash the read
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
        # Happy path: the whole file is a valid JSON array (this is the case for the Day 2 products file).
        doc = json.loads(text)
        objs = doc if isinstance(doc, list) else [doc]
        raw_objs = [json.dumps(o) for o in objs]
    except json.JSONDecodeError as e:
        # Day 1 products file: "Expecting ',' delimiter" at line 41 (two objects without a comma between them).
        # We record exactly where the defect is, then fall back to finding every {...} object with a regex.
        line_no = text.count("\n", 0, e.pos) + 1
        issue = f"JSON_STRUCTURAL_DEFECT: {e.msg} at line {line_no} col {e.colno} (char {e.pos}); salvaged object-by-object"
        raw_objs = re.findall(r"\{[^{}]*\}", text, flags=re.S)
    rows, bad = [], []
    for i, frag in enumerate(raw_objs, start=1):
        try:
            o = json.loads(frag)
        except json.JSONDecodeError:
            bad.append((i, frag))          # a fragment that is not valid JSON even on its own -> quarantine
            continue
        def s(k):
            # keep the source representation: strings as-is, booleans/numbers as JSON text, missing/null -> None
            # e.g. is_active may arrive as true (boolean), "true" (string), "FALSE", null, or be missing.
            if k not in o or o[k] is None:
                return None
            return o[k] if isinstance(o[k], str) else json.dumps(o[k])
        # One bronze row per product object: all fields as text + position + the full original JSON
        # + the original JSON type of is_active (so silver can report how many needed standardizing).
        rows.append(tuple(s(c) for c in SOURCE_COLUMNS["products"]) + (i, json.dumps(o),
                    "missing" if "is_active" not in o else ("null" if o["is_active"] is None else type(o["is_active"]).__name__)))
    schema = raw_schema("products").add("_row_num", "long").add("_raw_json", "string").add("_is_active_source_type", "string")
    return spark.createDataFrame(rows, schema), issue, bad

# COMMAND ----------

# MAIN LOOP: for every entity (branches, customers, products, transactions) find its file(s),
# archive them, read them raw, and write them to rb_bronze.<entity>.
log_rows = []
for entity, cfg in ENTITY_SOURCES.items():
    # Pick the files of this entity by name pattern (see ENTITY_SOURCES in includes/common).
    matched = [f for f in files if re.match(cfg["pattern"], f, flags=re.I)]
    if not matched:
        print(f"WARNING: no file for {entity} in {landing_dir}")
        continue
    dfs = []
    reset_quarantine(entity, "BRONZE")      # re-run safety: remove bronze-stage rejects of a previous run
    for fname in matched:
        path = archive(fname)
        issue = None
        if cfg["format"] == "csv":
            df = read_csv_raw(path, entity)
        else:
            df, issue, bad = read_json_array_raw(path)
            if bad:
                # Fragments that could not be parsed are not dropped: they go to quarantine with the raw text.
                bad_df = spark.createDataFrame([(BATCH_ID, fname, n, frag) for n, frag in bad],
                                               "_batch_id string, _source_file string, _row_num long, frag string")
                write_quarantine(bad_df.select("_batch_id", "_source_file", "_row_num",
                                               F.lit(None).cast("string").alias("record_key"),
                                               F.array(F.lit("JSON_UNPARSEABLE_FRAGMENT")).alias("reason_codes"),
                                               F.struct(F.col("frag").alias("fragment")).alias("_raw")), entity, "BRONZE")
        df = meta(df, fname)
        n = df.count()
        dfs.append(df)
        # One ingestion-log row per file: file name, paths, size, checksum, row count, structural issue.
        # (For JSON the row count also includes unparseable fragments, so received = loaded + quarantined.)
        log_rows.append((BATCH_ID, BATCH_SEQ, entity, fname, f"{landing_dir}/{fname}", path,
                         os.path.getsize(path), md5(path), n + (len(bad) if cfg["format"] != "csv" else 0), issue))
        print(f"{entity:13s} {fname:30s} rows={n} {issue or ''}")
    # Write all files of the entity for this batch. replaceWhere(_batch_id) makes re-runs safe.
    overwrite_partition(reduce(DataFrame.unionByName, dfs), f"{BRONZE}.{entity}")

# COMMAND ----------

# Store the ingestion metadata required by the brief: file name, row count, ingestion timestamp, batch id.
spark.sql(f"DELETE FROM {AUDIT}.ingestion_log WHERE batch_id = '{BATCH_ID}'")
(spark.createDataFrame(log_rows, """batch_id string, batch_seq int, entity string, source_file string, landing_path string,
                                     archive_path string, file_size_bytes long, file_md5 string, row_count long, structural_issues string""")
      .withColumn("ingested_at", F.current_timestamp())
      .write.mode("append").saveAsTable(f"{AUDIT}.ingestion_log"))
display(spark.table(f"{AUDIT}.ingestion_log").filter(F.col("batch_id") == BATCH_ID))
