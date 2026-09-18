#!/usr/bin/env python3
"""Spark job 02: build warning features from curated student-course rows."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build student warning features")
    parser.add_argument("--curated", default="/data/student/curated")
    parser.add_argument("--mart", default="/data/student/mart")
    return parser.parse_args()


def yes_flag(column: str):
    return F.when(F.lower(F.col(column)) == "yes", F.lit(1)).otherwise(F.lit(0))


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("student_warn_02_build_features")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    curated = args.curated.rstrip("/")
    mart = args.mart.rstrip("/")
    base = spark.read.parquet(f"{curated}/student_course_performance")

    course_stats = base.groupBy("course_id").agg(
        F.avg("g3").alias("course_avg_g3"),
        F.expr("percentile_approx(absences, 0.75)").alias("course_absence_p75"),
        F.count("*").alias("course_row_count"),
    )
    school_stats = base.groupBy("course_id", "school").agg(
        F.avg("g3").alias("school_avg_g3"),
        F.count("*").alias("school_row_count"),
    )
    course_window = Window.partitionBy("course_id")

    features = (
        base.join(course_stats, "course_id", "left")
        .join(school_stats, ["course_id", "school"], "left")
        .withColumn("grade_avg", (F.col("g1") + F.col("g2") + F.col("g3")) / F.lit(3.0))
        .withColumn("grade_trend", F.col("g3") - F.col("g1"))
        .withColumn(
            "absence_band",
            F.when(F.col("absences") >= 10, F.lit("HIGH"))
            .when(F.col("absences") >= 4, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn(
            "studytime_band",
            F.when(F.col("studytime") <= 1, F.lit("LOW"))
            .when(F.col("studytime") == 2, F.lit("MEDIUM"))
            .otherwise(F.lit("HIGH")),
        )
        .withColumn("past_failure_flag", F.col("failures") > 0)
        .withColumn(
            "support_count",
            yes_flag("schoolsup") + yes_flag("famsup") + yes_flag("paid"),
        )
        .withColumn("relative_to_course_avg", F.col("g3") - F.col("course_avg_g3"))
        .withColumn("course_rank_by_g3", F.percent_rank().over(course_window.orderBy("g3")))
        .select(
            "student_key",
            "school",
            "sex",
            "age",
            "address",
            "studytime",
            "failures",
            "schoolsup",
            "famsup",
            "paid",
            "higher",
            "internet",
            "absences",
            "g1",
            "g2",
            "g3",
            "grade_avg",
            "grade_trend",
            "absence_band",
            "studytime_band",
            "past_failure_flag",
            "support_count",
            "course_avg_g3",
            "school_avg_g3",
            "course_absence_p75",
            "relative_to_course_avg",
            "course_rank_by_g3",
            "course_row_count",
            "school_row_count",
            "course_id",
        )
    )

    (
        features.write.mode("overwrite")
        .format("parquet")
        .partitionBy("course_id")
        .save(f"{mart}/student_features")
    )

    print(f"02_build_features: wrote {features.count()} feature rows")
    spark.stop()


if __name__ == "__main__":
    main()
