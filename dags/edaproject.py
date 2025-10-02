# edaproject.py
from datetime import datetime, timedelta, timezone
import os, pathlib, json, math, random, re, html, unicodedata, gzip, csv
import numpy as np
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ===================== CONFIG (env first, then Airflow Variables fallback) =====================
S3_BUCKET         = os.environ.get("S3_BUCKET") or Variable.get("S3_BUCKET", default_var=None)
CSV_S3_URI        = os.environ.get("CSV_S3_URI") or Variable.get("CSV_S3_URI", default_var="")
BASE_PREFIX       = os.environ.get("BASE_PREFIX", "sentiment140")
OUT_DIR           = os.environ.get("OUT_DIR", "/tmp/sentiment140_eda_artifacts")    # EDA artifacts only
CLEAN_DIR         = os.environ.get("CLEAN_DIR", "/tmp/sentiment140_clean")          # (still used for EDA plots/tables if needed)
AWS_CONN_ID       = os.environ.get("AWS_CONN_ID", "aws_default")
SNOWFLAKE_CONN_ID = os.environ.get("SNOWFLAKE_CONN_ID", "snowflake_default")

# Snowflake identifiers (can also be set in the Snowflake connection Extra)
SF_DB       = os.environ.get("SNOWFLAKE_DATABASE",      Variable.get("SNOWFLAKE_DATABASE",      default_var="YOUR_DB"))
SF_SCHEMA   = os.environ.get("SNOWFLAKE_SCHEMA",        Variable.get("SNOWFLAKE_SCHEMA",        default_var="PUBLIC"))
SF_WH       = os.environ.get("SNOWFLAKE_WAREHOUSE",     Variable.get("SNOWFLAKE_WAREHOUSE",     default_var="YOUR_WH"))
SF_ROLE     = os.environ.get("SNOWFLAKE_ROLE",          Variable.get("SNOWFLAKE_ROLE",          default_var="YOUR_ROLE"))
SF_TABLE    = os.environ.get("SNOWFLAKE_TABLE_CLEAN",   Variable.get("SNOWFLAKE_TABLE_CLEAN",   default_var="SENTIMENT140_CLEAN"))

# Performance knobs
CHUNK_SIZE      = int(os.environ.get("CHUNK_SIZE",      Variable.get("CHUNK_SIZE",      default_var="100000")))
RESERVOIR_SIZE  = int(os.environ.get("RESERVOIR_SIZE",  Variable.get("RESERVOIR_SIZE",  default_var="200000")))

# -------- Local disk paths (inside the container) --------
LOCAL_IN_DIR   = os.environ.get("LOCAL_IN_DIR", "/tmp")
LOCAL_RAW_FILE = os.environ.get("LOCAL_RAW_FILE", "/tmp/train_raw.csv")
LOCAL_OUT_FILE = os.environ.get("LOCAL_OUT_FILE", "/tmp/train_clean.csv.gz")  # gz makes Snowflake loads faster


# Input CSV columns (Sentiment140)
COL_NAMES = ["sentiment","tweet_id","date","flag","user","text"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ===================== Helpers =====================
def _normalize_text(s: str) -> str:
    s = html.unescape(str(s))
    s = unicodedata.normalize("NFKC", s)
    s = s.lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s

url_pat     = re.compile(r"https?://\S+|www\.\S+")
mention_pat = re.compile(r"@\w+")
hashtag_pat = re.compile(r"#\w+")
emoji_pat   = re.compile(r"[\U0001F300-\U0001FAFF]")

class StreamingStats:
    """Streaming covariance/correlation using Welford updates."""
    def __init__(self, feature_names):
        self.n = 0
        self.means = np.zeros(len(feature_names), dtype=np.float64)
        self.M2 = np.zeros((len(feature_names), len(feature_names)), dtype=np.float64)
        self.fnames = feature_names
    def update(self, X):
        for x in X:
            self.n += 1
            d = x - self.means
            self.means += d / self.n
            self.M2 += np.outer(d, (x - self.means))
    def corr_df(self):
        if self.n < 2:
            return pd.DataFrame(np.zeros_like(self.M2), index=self.fnames, columns=self.fnames)
        cov = self.M2 / (self.n - 1)
        std = np.sqrt(np.diag(cov))
        denom = np.outer(std, std)
        with np.errstate(invalid='ignore', divide='ignore'):
            corr = np.where(denom == 0, 0, cov / denom)
        return pd.DataFrame(corr, index=self.fnames, columns=self.fnames)

# ===================== Task callables =====================
def _ensure_dirs(**_):
    pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(CLEAN_DIR).mkdir(parents=True, exist_ok=True)
    pathlib.Path(LOCAL_IN_DIR).mkdir(parents=True, exist_ok=True)
    # Ensure no leftover directory/file clash for raw/clean targets
    for p in (LOCAL_RAW_FILE, LOCAL_OUT_FILE):
        if os.path.isdir(p):
            import shutil
            shutil.rmtree(p)

def _download_raw_from_s3(**context):
    uri = CSV_S3_URI
    if not uri or not uri.startswith("s3://"):
        raise ValueError(
            "CSV_S3_URI is not set or invalid. Example: s3://298a1/sentiment140/airflow/main-data.csv"
        )
    print(f"[download_raw_from_s3] Using CSV_S3_URI={uri}")

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    bucket, key = uri.replace("s3://", "").split("/", 1)

    # Existence check for clearer error if the path is wrong
    if not hook.check_for_key(key=key, bucket_name=bucket):
        raise FileNotFoundError(f"S3 object not found: s3://{bucket}/{key}")

    # Make sure the parent directory exists
    target_dir = os.path.dirname(LOCAL_RAW_FILE) or "/tmp"
    pathlib.Path(target_dir).mkdir(parents=True, exist_ok=True)

    # If a directory was mistakenly created at the file path before, remove it
    if os.path.isdir(LOCAL_RAW_FILE):
        import shutil
        shutil.rmtree(LOCAL_RAW_FILE)
    # If a stale file exists, remove it
    elif os.path.isfile(LOCAL_RAW_FILE):
        os.remove(LOCAL_RAW_FILE)

    # >>> Use the underlying boto3 client to write to a concrete file path <<<
    s3_client = hook.get_conn()  # boto3 client
    s3_client.download_file(Bucket=bucket, Key=key, Filename=LOCAL_RAW_FILE)

    # Sanity-check
    if not os.path.isfile(LOCAL_RAW_FILE):
        raise FileNotFoundError(f"Download did not create a file: {LOCAL_RAW_FILE}")
    if os.path.getsize(LOCAL_RAW_FILE) == 0:
        raise IOError(f"Downloaded file is empty: {LOCAL_RAW_FILE}")

    print(f"[download_raw_from_s3] Downloaded to {LOCAL_RAW_FILE} ({os.path.getsize(LOCAL_RAW_FILE)} bytes)")
    context["ti"].xcom_push(key="csv_path", value=LOCAL_RAW_FILE)



def _run_eda_and_clean(**context):
    csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="download_raw_from_s3")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    context["ti"].xcom_push(key="run_ts", value=ts)

    # EDA accumulators
    char_bins = np.arange(0, 321, 4)
    word_bins = np.arange(0, 61, 1)
    hist_chars = np.zeros(len(char_bins)-1, dtype=np.int64)
    hist_words = np.zeros(len(word_bins)-1, dtype=np.int64)

    def add_to_hist(values, bins, hist_counts):
        c, _ = np.histogram(values, bins=bins)
        hist_counts += c

    from collections import Counter
    class_counts = Counter()
    empty_norm = 0
    feature_names = ['len_chars','len_words','n_urls','n_mentions','n_hashtags','n_emojis']
    sstats = StreamingStats(feature_names)
    reservoir_chars, reservoir_words = [], []
    rows_seen = 0

    # Robust chunked reader
    try:
        chunk_iter = pd.read_csv(
            csv_path, encoding="latin-1", header=None, names=COL_NAMES,
            dtype="string", engine="c", on_bad_lines="skip",
            sep=",", quotechar='"', doublequote=True, escapechar="\\",
            skipinitialspace=True, chunksize=CHUNK_SIZE, low_memory=False
        )
    except Exception:
        chunk_iter = pd.read_csv(
            csv_path, encoding="latin-1", header=None, names=COL_NAMES,
            dtype="string", engine="python", on_bad_lines="skip",
            sep=",", quotechar='"', doublequote=True, escapechar="\\",
            skipinitialspace=True, chunksize=CHUNK_SIZE
        )

    # Cleaned single file: write to LOCAL_OUT_FILE (gzip if endswith .gz)
    pathlib.Path(os.path.dirname(LOCAL_OUT_FILE)).mkdir(parents=True, exist_ok=True)

    def _open_for_write(path):
        if path.endswith(".gz"):
            return gzip.open(path, "wt", encoding="utf-8", newline="")
        return open(path, "w", encoding="utf-8", newline="")

    gf = _open_for_write(LOCAL_OUT_FILE)
    gw = csv.writer(gf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    gw.writerow(["label","text"])


    for chunk in chunk_iter:
        # Filter labels {0,4}
        chunk['sentiment'] = pd.to_numeric(chunk['sentiment'], errors='coerce')
        chunk = chunk[chunk['sentiment'].isin([0,4])]

        # Text presence
        chunk['text'] = chunk['text'].fillna("")
        chunk = chunk[chunk['text'].str.strip() != ""]
        if chunk.empty:
            continue

        # Normalize + features
        chunk['text_norm']  = chunk['text'].apply(_normalize_text)
        chunk['len_chars']  = chunk['text_norm'].str.len().astype('Int32')
        chunk['len_words']  = chunk['text_norm'].str.split().apply(len).astype('Int32')
        chunk['n_urls']     = chunk['text_norm'].str.count(url_pat).astype('Int16')
        chunk['n_mentions'] = chunk['text_norm'].str.count(mention_pat).astype('Int16')
        chunk['n_hashtags'] = chunk['text_norm'].str.count(hashtag_pat).astype('Int16')
        chunk['n_emojis']   = chunk['text_norm'].str.count(emoji_pat).astype('Int16')

        labels = chunk['sentiment'].map({0:0, 4:1})
        class_counts.update(labels.tolist())
        empty_norm += int((chunk['len_chars'] == 0).sum())

        add_to_hist(chunk['len_chars'].to_numpy(), char_bins, hist_chars)
        add_to_hist(chunk['len_words'].to_numpy(), word_bins, hist_words)

        X = chunk[feature_names].astype(np.float64).to_numpy()
        sstats.update(X)

        # write cleaned rows (sanitize text to be single-line, safe for CSV)
        # - replace CR/LF with space
        # - remove NULLs just in case
        for lbl, txt in zip(labels.tolist(), chunk['text_norm'].tolist()):
            t = txt.replace("\r", " ").replace("\n", " ").replace("\x00", "")
            gw.writerow([int(lbl), t])

        # reservoir for IQR (approx)
        for c, w in zip(chunk['len_chars'].tolist(), chunk['len_words'].tolist()):
            if len(reservoir_chars) < RESERVOIR_SIZE:
                reservoir_chars.append(int(c)); reservoir_words.append(int(w))
            else:
                j = random.randint(0, rows_seen)
                if j < RESERVOIR_SIZE:
                    reservoir_chars[j] = int(c); reservoir_words[j] = int(w)
            rows_seen += 1

    gf.close()

    # IQR bounds
    if reservoir_chars:
        q1_c, q3_c = np.percentile(reservoir_chars, [25, 75])
        iqr_c = q3_c - q1_c
        lower_c = max(0, int(math.floor(q1_c - 1.5*iqr_c)))
        upper_c = int(math.ceil(q3_c + 1.5*iqr_c))
    else:
        q1_c = q3_c = iqr_c = lower_c = upper_c = 0

    # Save EDA tables and plots under OUT_DIR
    pathlib.Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
    class_df = pd.DataFrame({"label":[0,1], "count":[class_counts.get(0,0), class_counts.get(1,0)]})
    class_csv = os.path.join(OUT_DIR, f"class_balance_{ts}.csv"); class_df.to_csv(class_csv, index=False)
    corr_df = sstats.corr_df()
    corr_csv = os.path.join(OUT_DIR, f"feature_correlations_{ts}.csv"); corr_df.to_csv(corr_csv)

    def save_plot_hist_from_counts(bins, counts, title, xlabel, out_path):
        import matplotlib.pyplot as plt
        widths = np.diff(bins); centers = bins[:-1] + widths/2.0
        fig = plt.figure(); plt.bar(centers, counts, width=widths, align="center")
        plt.title(title); plt.xlabel(xlabel); plt.ylabel("Frequency"); plt.tight_layout()
        fig.savefig(out_path, dpi=150); plt.close(fig)
    def save_plot_bar(keys, vals, title, xlabel, ylabel, out_path):
        import matplotlib.pyplot as plt
        fig = plt.figure()
        plt.bar(range(len(keys)), vals)
        plt.title(title); plt.xlabel(xlabel); plt.ylabel(ylabel)
        plt.xticks(range(len(keys)), keys); plt.tight_layout()
        fig.savefig(out_path, dpi=150); plt.close(fig)

    chars_png = os.path.join(OUT_DIR, f"hist_len_chars_{ts}.png")
    words_png = os.path.join(OUT_DIR, f"hist_len_words_{ts}.png")
    balance_png = os.path.join(OUT_DIR, f"class_balance_{ts}.png")
    save_plot_hist_from_counts(char_bins, hist_chars, "Tweet Character Lengths", "Characters", chars_png)
    save_plot_hist_from_counts(word_bins, hist_words, "Tweet Word Counts", "Words", words_png)
    save_plot_bar([0,1], [class_counts.get(0,0), class_counts.get(1,0)], "Class Balance (0=neg,1=pos)", "Label", "Count", balance_png)

    # Save summary JSON
    summary = {
        "generated_at_utc": ts,
        "class_balance": {"negatives": int(class_counts.get(0,0)), "positives": int(class_counts.get(1,0))},
        "iqr_bounds_len_chars": {"lower": int(lower_c), "upper": int(upper_c), "q1": float(q1_c), "q3": float(q3_c)},
        "artifacts": {
            "class_csv": os.path.basename(class_csv),
            "corr_csv": os.path.basename(corr_csv),
            "hist_chars_png": os.path.basename(chars_png),
            "hist_words_png": os.path.basename(words_png),
            "class_balance_png": os.path.basename(balance_png),
            "cleaned_output": os.path.basename(LOCAL_OUT_FILE),
        }
    }
    with open(os.path.join(OUT_DIR, f"eda_summary_{ts}.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # pass cleaned local path to next task
    context["ti"].xcom_push(key="cleaned_local_path", value=LOCAL_OUT_FILE)

def _upload_to_s3(**context):
    assert S3_BUCKET, "S3_BUCKET must be set"
    ts = context["ti"].xcom_pull(key="run_ts", task_ids="run_eda_and_clean")
    cleaned_local_path = context["ti"].xcom_pull(key="cleaned_local_path", task_ids="run_eda_and_clean")
    if not cleaned_local_path or not os.path.isfile(cleaned_local_path):
        raise FileNotFoundError(f"Cleaned file not found at {cleaned_local_path}")

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # EDA artifacts -> s3://<bucket>/<BASE_PREFIX>/eda/<ts>/
    eda_prefix = f"{BASE_PREFIX}/eda/{ts}"
    for p in pathlib.Path(OUT_DIR).glob("*"):
        if p.is_file():
            key = f"{eda_prefix}/{p.name}"
            print(f"Uploading EDA: {p} -> s3://{S3_BUCKET}/{key}")
            hook.load_file(filename=str(p), key=key, bucket_name=S3_BUCKET, replace=True)

    # Cleaned -> s3://<bucket>/<BASE_PREFIX>/cleaned/<ts>/<basename>
    clean_prefix = f"{BASE_PREFIX}/cleaned/{ts}"
    clean_key = f"{clean_prefix}/{os.path.basename(cleaned_local_path)}"
    print(f"Uploading CLEAN: {cleaned_local_path} -> s3://{S3_BUCKET}/{clean_key}")
    hook.load_file(filename=cleaned_local_path, key=clean_key, bucket_name=S3_BUCKET, replace=True)

    # pass prefix for Snowflake load (COPY will match the single file)
    context["ti"].xcom_push(key="clean_s3_prefix", value=clean_prefix)

def _create_table_if_needed(**_):
    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sf.run([
        f"USE WAREHOUSE {SF_WH};",
        f"USE ROLE {SF_ROLE};",
        f"USE DATABASE {SF_DB};",
        f"USE SCHEMA {SF_SCHEMA};",
        f"""
        CREATE TABLE IF NOT EXISTS {SF_DB}.{SF_SCHEMA}.{SF_TABLE} (
            LABEL INTEGER,
            TEXT  STRING
        );
        """
    ])

def _copy_clean_into_snowflake(**context):
    clean_prefix = context["ti"].xcom_pull(key="clean_s3_prefix", task_ids="upload_to_s3")
    assert clean_prefix, "Missing clean_s3_prefix"

    # Use AWS creds from S3Hook (or switch to STORAGE INTEGRATION if you have one)
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    creds = s3_hook.get_credentials()
    aws_key = creds.access_key
    aws_secret = creds.secret_key
    aws_token = creds.token  # may be None

    stage_path = f"s3://{S3_BUCKET}/{clean_prefix}/"
    cred_sql = f"CREDENTIALS=(AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret}'"
    if aws_token:
        cred_sql += f" AWS_TOKEN='{aws_token}'"
    cred_sql += ")"

    copy_sql = f"""
    COPY INTO {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
    FROM '{stage_path}'
    {cred_sql}
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_DELIMITER = ','
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        ESCAPE = '\\\\'
        ESCAPE_UNENCLOSED_FIELD = NONE
        SKIP_HEADER = 1
        TRIM_SPACE = FALSE
        NULL_IF = ()
        COMPRESSION = AUTO
    )
    PATTERN = '.*'
    ON_ERROR = 'CONTINUE';
    """

# and then chain: copy_clean_into_snowflake >> sf_copy_check

    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sf.run([
        f"USE WAREHOUSE {SF_WH};",
        f"USE ROLE {SF_ROLE};",
        f"USE DATABASE {SF_DB};",
        f"USE SCHEMA {SF_SCHEMA};",
        copy_sql
    ])
    
def _sf_debug_and_assert_table(**_):
    """Print Snowflake session context and assert the target table exists."""
    sf = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = sf.get_conn()
    cur = conn.cursor()
    try:
        cur.execute(f"USE ROLE {SF_ROLE}")
        cur.execute(f"USE WAREHOUSE {SF_WH}")
        cur.execute(f"USE DATABASE {SF_DB}")
        cur.execute(f"USE SCHEMA {SF_SCHEMA}")

        cur.execute("""
            SELECT CURRENT_ACCOUNT(), CURRENT_REGION(),
                CURRENT_ROLE(), CURRENT_WAREHOUSE(),
                CURRENT_DATABASE(), CURRENT_SCHEMA()
        """)
        print("CONTEXT:", cur.fetchall())

        cur.execute(f"SHOW TABLES IN SCHEMA {SF_DB}.{SF_SCHEMA}")
        names = {row[1].upper() for row in cur.fetchall()}  # row[1] is table_name
        print("TABLES:", names)
        if SF_TABLE.upper() not in names:
            raise ValueError(f"Table not found: {SF_DB}.{SF_SCHEMA}.{SF_TABLE}")
    finally:
        cur.close()
        conn.close()



# ===================== DAG & Tasks =====================
with DAG(
    dag_id="sentiment140_end2end_single",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # trigger manually; set @daily later if needed
    catchup=False,
    default_args=default_args,
    description="One-file DAG: S3 RAW -> EDA+Clean -> S3 Cleaned -> Snowflake load + checks",
) as dag:

    ensure_dirs = PythonOperator(task_id="ensure_dirs", python_callable=_ensure_dirs)

    download_raw_from_s3 = PythonOperator(
        task_id="download_raw_from_s3",
        python_callable=_download_raw_from_s3,
        provide_context=True
    )

    run_eda_and_clean = PythonOperator(
        task_id="run_eda_and_clean",
        python_callable=_run_eda_and_clean,
        provide_context=True
    )

    upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=_upload_to_s3,
        provide_context=True
    )

    create_table_if_needed = PythonOperator(
        task_id="create_table_if_needed",
        python_callable=_create_table_if_needed
    )

    copy_clean_into_snowflake = PythonOperator(
        task_id="copy_clean_into_snowflake",
        python_callable=_copy_clean_into_snowflake,
        provide_context=True
    )
    
    sf_debug_and_assert_table = PythonOperator(
        task_id="sf_debug_and_assert_table",
        python_callable=_sf_debug_and_assert_table
    )


    # Lightweight Snowflake checks (results appear in task logs)
    sf_check_rowcount = SQLExecuteQueryOperator(
        task_id="sf_check_rowcount",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE ROLE {SF_ROLE};
            USE WAREHOUSE {SF_WH};
            USE DATABASE {SF_DB};
            USE SCHEMA {SF_SCHEMA};
            SELECT COUNT(*) AS ROW_COUNT FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE};
        """,
    )

    sf_check_label_dist = SQLExecuteQueryOperator(
        task_id="sf_check_label_dist",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            USE ROLE {SF_ROLE};
            USE WAREHOUSE {SF_WH};
            USE DATABASE {SF_DB};
            USE SCHEMA {SF_SCHEMA};
            SELECT LABEL, COUNT(*) AS CNT
            FROM {SF_DB}.{SF_SCHEMA}.{SF_TABLE}
            GROUP BY 1
            ORDER BY 1;
        """,
    )


    # Flow
    ensure_dirs >> download_raw_from_s3 >> run_eda_and_clean >> upload_to_s3
    upload_to_s3 >> create_table_if_needed >> copy_clean_into_snowflake \
        >> sf_debug_and_assert_table >> [sf_check_rowcount, sf_check_label_dist]
