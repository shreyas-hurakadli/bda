# Student Performance Warning System

Big-data course project that uses Hadoop HDFS, Spark, and Hive to identify
students who may need academic intervention. The project is intentionally
focused on data engineering and analytics, not machine-learning model accuracy.

## Stack

- Hadoop HDFS for distributed storage
- Spark for ETL, feature building, and rule-based warning scores
- Hive for external tables, views, and SQL demo queries

## Installation Policy

Do not install Hadoop, Spark, Hive, Java, or project dependencies directly on the
host system. For local demos, use the project-scoped container flow. The host
only needs Docker or Podman.

## Dataset

The dataset is in `student+performance/student/`:

- `student-mat.csv`: Math course records
- `student-por.csv`: Portuguese course records
- `student.txt`: column dictionary

Both CSV files are semicolon-delimited and have 33 shared columns. Spark adds
`course_id=MAT` or `course_id=POR` during ingestion.

## Pipeline

```text
local CSV -> HDFS landing -> Spark curated Parquet -> Spark mart Parquet
  -> Hive external tables/views -> demo SQL warnings
```

## HDFS Paths

- Landing: `/data/student/landing/`
- Curated: `/data/student/curated/student_course_performance/`
- Mart features: `/data/student/mart/student_features/`
- Mart risk flags: `/data/student/mart/risk_flags/`

## Run The Demo Locally

From the repository root:

```bash
bash scripts/demo_container.sh
```

The script performs:

1. Build a local container image containing Hadoop, Spark, and Hive.
2. Mount this project into the container at `/workspace`.
3. Start single-node HDFS inside the container.
4. Initialize a project-scoped Hive Derby metastore in `.runtime/`.
5. Run `scripts/demo.sh` inside the container.

Generated runtime state stays under `.runtime/` and is ignored by git.

## Manual Commands For A Lab Cluster

Use these only when Hadoop, Spark, and Hive are already provided by your lab or
college cluster:

```bash
hdfs dfs -mkdir -p /data/student/landing
hdfs dfs -put -f student+performance/student/student-mat.csv /data/student/landing/
hdfs dfs -put -f student+performance/student/student-por.csv /data/student/landing/

spark-submit spark/01_ingest_clean.py
spark-submit spark/02_build_features.py
spark-submit spark/03_risk_flags.py

hive -f hive/01_create_db.hql
hive -f hive/02_curated_tables.hql
hive -f hive/03_warning_views.hql
hive -f hive/04_demo_queries.hql
```

## Container Runtime Files

- `infra/container/Containerfile`: builds a project image with Hadoop, Spark,
  and Hive.
- `infra/container/hadoop/`: HDFS and Hadoop client config.
- `infra/container/hive/`: embedded Hive metastore config.
- `infra/container/run-demo.sh`: starts HDFS inside the container and runs the
  pipeline.
- `scripts/demo_container.sh`: host wrapper for Docker/Podman.

## Warning Logic

The warning system uses transparent rules:

- `G3 < 10`: low final grade
- `G3 - G1 <= -3`: declining grade trend
- `absences >= max(10, course 75th percentile)`: high absences
- `failures > 0`: previous failures
- `studytime <= 1`: low study time

Risk levels:

- `HIGH`: 3 or more warning flags
- `MEDIUM`: 2 warning flags
- `LOW`: 0 or 1 warning flag

## Review Talking Points

- Hadoop stores raw, curated, and mart data in separate HDFS zones.
- Spark converts raw CSV to typed Parquet and computes warning signals at scale.
- Parquet plus `course_id` partitions demonstrate big-data storage best practices.
- Hive external tables expose the processed data for SQL analytics.
- The demo queries show at-risk students, course-level risk, school-level risk,
  and rule contribution counts.
