# Student Performance Prediction & Warning System

## Goal
Build a review-ready big-data demo that ingests the UCI student performance
dataset, stores it on HDFS, transforms it with Spark, exposes analytics through
Hive, and flags at-risk students using transparent rules.

## Hard Constraints
- Allowed stack only: Hadoop HDFS, Spark, Hive.
- Do not install Hadoop, Spark, Hive, Java, or related project dependencies
  directly on the host system.
- Prefer the project-scoped container flow in `scripts/demo_container.sh`.
- Host dependency should be limited to Docker or Podman for local demos.
- Big-data workflow is the focus; do not add external ML frameworks, databases,
  dashboards, Kafka, REST APIs, or web apps as core dependencies.
- Use rule-based warning logic, not model-training accuracy, as the prediction
  story.
- Keep student identifiers anonymized; never add real PII.

## Dataset
- Source folder: `student+performance/student/`
- Files:
  - `student-mat.csv`: Math course rows
  - `student-por.csv`: Portuguese course rows
  - `student.txt`: dataset dictionary
- CSV format: semicolon-delimited, header row, quoted string fields.
- Both course files share 33 columns: demographics, family context, support,
  behavior, absences, and grades `G1`, `G2`, `G3`.
- Add derived `course_id`: `MAT` for Math, `POR` for Portuguese.

## Architecture
local CSV files -> HDFS landing -> Spark clean/type -> HDFS curated Parquet
  -> Spark features/risk flags -> HDFS mart Parquet -> Hive external tables
  -> demo SQL warning reports

## HDFS Layers
1. Landing: raw CSVs under `/data/student/landing/`
2. Curated: typed Parquet under `/data/student/curated/student_course_performance/`
3. Mart: features and risk flags under `/data/student/mart/`
4. Hive: external tables over curated and mart paths

## Warning Rules
Risk is interpretable and computed in Spark:
- Low final grade: `G3 < 10`
- Declining grade: `G3 - G1 <= -3`
- High absences: course 75th percentile or at least 10 absences
- Past failures: `failures > 0`
- Low study time: `studytime <= 1`
- `risk_level`: `LOW`, `MEDIUM`, `HIGH`

## Conventions
- Prefer Parquet for curated and mart data.
- Partition curated and mart outputs by `course_id`.
- Keep Spark jobs idempotent and rerunnable.
- Keep Hive DDL in `hive/`, Spark jobs in `spark/`, demo commands in `scripts/`.
- Use `README.md` as the review script and explanation.
- Keep generated runtime state in `.runtime/`; do not commit metastore,
  warehouse, HDFS data directories, or logs.

## Demo Flow
1. Run `bash scripts/demo_container.sh` for local reviews.
2. The container starts project-scoped HDFS, Spark, Hive, and Derby metastore.
3. Upload `student-mat.csv` and `student-por.csv` to HDFS landing.
4. Run Spark ingest, feature, and risk jobs.
5. Create/refresh Hive external tables and views.
6. Run demo SQL for at-risk students, course risk, and school risk.
7. Show HDFS paths, Spark job output, Hive tables, and query results as proof of
   the big-data pipeline.
