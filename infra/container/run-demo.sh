#!/usr/bin/env bash
set -euo pipefail

cd /workspace

export HADOOP_ROOT_LOGGER="${HADOOP_ROOT_LOGGER:-WARN,console}"
export HADOOP_OPTS="${HADOOP_OPTS:-} -Djava.net.preferIPv4Stack=true"
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-1024}"
export HADOOP_CLIENT_OPTS="${HADOOP_CLIENT_OPTS:-} -Xmx1024m"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python3

mkdir -p .runtime/hdfs/name .runtime/hdfs/data .runtime/hive /tmp/hive

cleanup() {
  hdfs --daemon stop datanode >/dev/null 2>&1 || true
  hdfs --daemon stop namenode >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ ! -d .runtime/hdfs/name/current ]; then
  echo "Formatting project-scoped HDFS namenode in .runtime/"
  hdfs namenode -format -force -nonInteractive
fi

echo "Starting HDFS services inside the container"
hdfs --daemon start namenode
hdfs --daemon start datanode

for attempt in $(seq 1 30); do
  if hdfs dfs -ls / >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "HDFS did not become ready" >&2
    exit 1
  fi
  sleep 2
done

echo "Ensuring HDFS is writable"
hdfs dfsadmin -safemode leave || true
hdfs dfsadmin -safemode wait

hdfs dfs -mkdir -p /tmp/hive /user/hive/warehouse
hdfs dfs -chmod -R 777 /tmp /user/hive

if [ ! -d .runtime/hive/metastore_db ]; then
  echo "Initializing project-scoped Hive Derby metastore in .runtime/"
  schematool -dbType derby -initSchema
fi

bash scripts/demo.sh
