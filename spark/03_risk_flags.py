#!/usr/bin/env python3
"""Spark job 03: compute rule-based early warning flags."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


LOW_FINAL_GRADE = 10
DECLINE_POINTS = -3
ABSENCE_MINIMUM = 10
LOW_STUDYTIME = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flag at-risk students")
    parser.add_argument("--mart", default="/data/student/mart")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("student_warn_03_risk_flags")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    mart = args.mart.rstrip("/")
    features = spark.read.parquet(f"{mart}/student_features")

    flagged = (
        features.withColumn("flag_low_final_grade", F.col("g3") < LOW_FINAL_GRADE)
        .withColumn("flag_declining_grade", F.col("grade_trend") <= DECLINE_POINTS)
        .withColumn(
            "flag_high_absences",
            (F.col("absences") >= ABSENCE_MINIMUM)
            | (F.col("absences") >= F.col("course_absence_p75")),
        )
        .withColumn("flag_previous_failures", F.col("failures") > 0)
        .withColumn("flag_low_studytime", F.col("studytime") <= LOW_STUDYTIME)
        .withColumn(
            "risk_score",
            F.col("flag_low_final_grade").cast("int")
            + F.col("flag_declining_grade").cast("int")
            + F.col("flag_high_absences").cast("int")
            + F.col("flag_previous_failures").cast("int")
            + F.col("flag_low_studytime").cast("int"),
        )
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 3, F.lit("HIGH"))
            .when(F.col("risk_score") == 2, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn(
            "warning_reasons",
            F.concat_ws(
                ",",
                F.when(F.col("flag_low_final_grade"), F.lit("low_final_grade")),
                F.when(F.col("flag_declining_grade"), F.lit("declining_grade")),
                F.when(F.col("flag_high_absences"), F.lit("high_absences")),
                F.when(F.col("flag_previous_failures"), F.lit("previous_failures")),
                F.when(F.col("flag_low_studytime"), F.lit("low_studytime")),
            ),
        )
        .select(
            "student_key",
            "school",
            "sex",
            "age",
            "studytime",
            "failures",
            "absences",
            "g1",
            "g2",
            "g3",
            "grade_avg",
            "grade_trend",
            "absence_band",
            "studytime_band",
            "course_avg_g3",
            "school_avg_g3",
            "course_absence_p75",
            "relative_to_course_avg",
            "risk_score",
            "risk_level",
            "warning_reasons",
            "flag_low_final_grade",
            "flag_declining_grade",
            "flag_high_absences",
            "flag_previous_failures",
            "flag_low_studytime",
            "course_id",
        )
    )

    (
        flagged.write.mode("overwrite")
        .format("parquet")
        .partitionBy("course_id")
        .save(f"{mart}/risk_flags")
    )

    print(f"03_risk_flags: wrote {flagged.count()} risk rows")
    spark.stop()


if __name__ == "__main__":
    main()
