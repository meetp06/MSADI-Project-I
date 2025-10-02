## Overview

This project builds an **end-to-end data pipeline** for the Sentiment140 dataset using **Apache Airflow**, **Amazon S3**, and **Snowflake**.

It automates:

* Downloading raw tweet data from **S3**
* Performing **Exploratory Data Analysis (EDA)**
* Cleaning and normalizing tweets
* Uploading EDA artifacts and cleaned datasets back to S3
* Loading the final cleaned dataset into **Snowflake** for downstream analytics

---

## How It Works

* **Step 1:** Download raw CSV from S3 (`CSV_S3_URI`) into the container (`/tmp/train_raw.csv`).
* **Step 2:** Perform EDA and preprocessing:

  * Keep only sentiment labels {0,4}, map to {0,1}
  * Normalize tweets (HTML unescape, lowercase, whitespace cleanup)
  * Sanitize text for CSV (remove CR/LF/NULL characters)
  * Generate plots and summaries: histograms, class balance, correlations, JSON summary
* **Step 3:** Save the cleaned dataset as `train_clean.csv`.
* **Step 4:** Upload results to S3 in timestamped folders:

  * EDA artifacts → `s3://<bucket>/sentiment140/eda/<timestamp>/`
  * Clean dataset → `s3://<bucket>/sentiment140/cleaned/<timestamp>/train_clean.csv`
* **Step 5:** Create a Snowflake table if missing and run `COPY INTO` to load the cleaned data.
* **Step 6:** Run SQL checks in Snowflake (row count, label distribution).

---

## Files Generated

| File / Path                     | Description                                            |
| :------------------------------ | :----------------------------------------------------- |
| `train_clean.csv`               | Cleaned, model-ready dataset (`label,text`)            |
| `class_balance_<ts>.csv`        | CSV summary of class counts                            |
| `feature_correlations_<ts>.csv` | Correlation matrix of EDA features                     |
| `hist_len_chars_<ts>.png`       | Histogram of tweet lengths (characters)                |
| `hist_len_words_<ts>.png`       | Histogram of tweet lengths (words)                     |
| `class_balance_<ts>.png`        | Class balance bar chart                                |
| `eda_summary_<ts>.json`         | JSON metadata with bounds, counts, artifact references |
| `edaproject.py`                 | Airflow DAG orchestrating the entire pipeline          |

---

## How to Run

### 1. Prerequisites

* Docker & Docker Compose
* AWS S3 bucket with raw Sentiment140 CSV, e.g.:

  ```
  s3://298a1/sentiment140/airflow/main-data.csv
  ```
* Snowflake account with:

  * Warehouse (`COMPUTE_WH`)
  * Database (`SENTIMENT_DB`)
  * Schema (`PUBLIC`)
  * Role with sufficient privileges (`ACCOUNTADMIN` for dev/test)

---

### 2. Setup Airflow Providers

In `docker-compose.yml` make sure `_PIP_ADDITIONAL_REQUIREMENTS` includes:

```text
apache-airflow-providers-amazon
apache-airflow-providers-snowflake
snowflake-connector-python
boto3
pandas
numpy
matplotlib
```

---

### 3. Configure Environment Variables

Set environment for both webserver & scheduler in `docker-compose.yml`:

```yaml
environment:
  - S3_BUCKET=298a1
  - CSV_S3_URI=s3://298a1/sentiment140/airflow/main-data.csv
  - BASE_PREFIX=sentiment140
  - OUT_DIR=/tmp/sentiment140_eda_artifacts
  - CLEAN_DIR=/tmp/sentiment140_clean
  - AWS_CONN_ID=aws_default
  - SNOWFLAKE_CONN_ID=snowflake_default
  - SNOWFLAKE_DATABASE=SENTIMENT_DB
  - SNOWFLAKE_SCHEMA=PUBLIC
  - SNOWFLAKE_WAREHOUSE=COMPUTE_WH
  - SNOWFLAKE_ROLE=ACCOUNTADMIN
  - SNOWFLAKE_TABLE_CLEAN=SENTIMENT140_CLEAN
  # optional tuning
  - CHUNK_SIZE=100000
  - RESERVOIR_SIZE=200000
  - LOCAL_IN_DIR=/tmp
  - LOCAL_RAW_FILE=/tmp/train_raw.csv
  - LOCAL_OUT_FILE=/tmp/train_clean.csv
```

---

### 4. Configure Airflow Connections

In Airflow UI → **Admin → Connections**:

* **`aws_default`**

  * Type: Amazon Web Services
  * Auth: AWS access key/secret
  * Extra: `{"region_name":"us-west-1"}`

* **`snowflake_default`**

  * Type: Snowflake
  * Account: full account locator (e.g. `xy12345.us-east-1`)
  * User / Password
  * Warehouse: `COMPUTE_WH`
  * Database: `SENTIMENT_DB`
  * Schema: `PUBLIC`
  * Role: `ACCOUNTADMIN`

---

### 5. Run Airflow

```bash
docker compose up -d
# Open Airflow UI at http://localhost:8080 (default user/pass: airflow / airflow)
```

---

### 6. Trigger the DAG

DAG ID: **`sentiment140_end2end_single`**

* From Airflow UI: trigger DAG manually
* Or CLI:

  ```bash
  docker compose exec airflow-webserver airflow dags trigger sentiment140_end2end_single
  ```

---

## Workflow

### DAG Flow

```
ensure_dirs
 → download_raw_from_s3
 → run_eda_and_clean
 → upload_to_s3
 → create_table_if_needed
 → copy_clean_into_snowflake
 → sf_debug_and_assert_table
 → sf_check_rowcount, sf_check_label_dist
```

### Data Flow

```
S3 (raw CSV)
   │
   ▼
Airflow DAG
 ├─ EDA (plots, summaries)
 ├─ Cleaning (normalize, sanitize)
 ├─ Upload artifacts + clean CSV to S3
 └─ COPY INTO Snowflake
       │
       ▼
Snowflake table: SENTIMENT140_CLEAN
```

---


## License

This project is for **educational/demo purposes**.
You are free to adapt it for coursework or prototypes.

