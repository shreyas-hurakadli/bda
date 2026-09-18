#!/usr/bin/env bash
set -euo pipefail

HDFS_ROOT="${HDFS_ROOT:-/data/student}"
LANDING="${HDFS_ROOT}/landing"
CURATED="${HDFS_ROOT}/curated"
MART="${HDFS_ROOT}/mart"

LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-student+performance/student}"

echo "== Student Performance Warning System Demo =="
echo "HDFS root: ${HDFS_ROOT}"
echo

echo "== 1. Prepare HDFS landing zone =="
hdfs dfs -mkdir -p "${LANDING}" "${CURATED}" "${MART}"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/student-mat.csv" "${LANDING}/"
hdfs dfs -put -f "${LOCAL_DATA_DIR}/student-por.csv" "${LANDING}/"
hdfs dfs -ls "${LANDING}"
echo

echo "== 2. Run Spark ETL and warning jobs =="
spark-submit spark/01_ingest_clean.py --landing "${LANDING}" --curated "${CURATED}"
spark-submit spark/02_build_features.py --curated "${CURATED}" --mart "${MART}"
spark-submit spark/03_risk_flags.py --mart "${MART}"
echo

echo "== 3. Show HDFS output zones =="
hdfs dfs -du -h "${HDFS_ROOT}" || true
hdfs dfs -ls -R "${HDFS_ROOT}"
echo

echo "== 4. Create/refresh Hive tables and views =="
hive -f hive/01_create_db.hql
hive -f hive/02_curated_tables.hql
hive -f hive/03_warning_views.hql
echo

echo "== 5. Run review demo queries =="
hive -f hive/04_demo_queries.hql

echo
echo "Demo complete."
