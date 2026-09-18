#!/usr/bin/env python3
"""Spark job 01: ingest raw UCI student CSVs -> curated Parquet.

Reads the Math and Portuguese course files from HDFS landing, casts columns, adds
`course_id`, and writes one partitioned curated dataset.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


STRING_COLS = [
    "school",
    "sex",
    "address",
    "famsize",
    "pstatus",
    "mjob",
    "fjob",
    "reason",
    "guardian",
    "schoolsup",
    "famsup",
    "paid",
    "activities",
    "nursery",
    "higher",
    "internet",
    "romantic",
]

INT_COLS = [
    "age",
    "medu",
    "fedu",
    "traveltime",
    "studytime",
    "failures",
    "famrel",
    "freetime",
    "goout",
    "dalc",
    "walc",
    "health",
    "absences",
    "g1",
    "g2",
    "g3",
]

RAW_TO_CANONICAL = {
    "Pstatus": "pstatus",
    "Medu": "medu",
    "Fedu": "fedu",
    "Mjob": "mjob",
    "Fjob": "fjob",
    "Dalc": "dalc",
    "Walc": "walc",
    "G1": "g1",
    "G2": "g2",
    "G3": "g3",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest UCI student performance data")
    parser.add_argument("--landing", default="/data/student/landing")
    parser.add_argument("--curated", default="/data/student/curated")
    return parser.parse_args()


def read_course(spark: SparkSession, path: str, course_id: str) -> DataFrame:
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("quote", '"')
        .option("escape", '"')
        .option("inferSchema", False)
        .csv(path)
    )

    for raw_col, canonical_col in RAW_TO_CANONICAL.items():
        raw = raw.withColumnRenamed(raw_col, canonical_col)

    selected = [F.trim(F.col(c)).alias(c) for c in STRING_COLS] + [
        F.col(c).cast("int").alias(c) for c in INT_COLS
    ]

    return (
        raw.select(*selected)
        .withColumn("course_id", F.lit(course_id))
        .withColumn("source_file", F.lit(path.rsplit("/", 1)[-1]))
        .withColumn(
            "student_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    "school",
                    "sex",
                    F.col("age").cast("string"),
                    "address",
                    "famsize",
                    "pstatus",
                    F.col("medu").cast("string"),
                    F.col("fedu").cast("string"),
                    "mjob",
                    "fjob",
                    "reason",
                    "guardian",
                ),
                256,
            ),
        )
    )


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("student_warn_01_ingest_clean")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    landing = args.landing.rstrip("/")
    curated = args.curated.rstrip("/")

    math = read_course(spark, f"{landing}/student-mat.csv", "MAT")
    portuguese = read_course(spark, f"{landing}/student-por.csv", "POR")
    cleaned = math.unionByName(portuguese).filter(F.col("g3").isNotNull())

    (
        cleaned.write.mode("overwrite")
        .format("parquet")
        .partitionBy("course_id")
        .save(f"{curated}/student_course_performance")
    )

    print(f"01_ingest_clean: wrote {cleaned.count()} curated student-course rows")
    spark.stop()


if __name__ == "__main__":
    main()
