-- Mart tables + warning views for demos
USE student_warn;

DROP TABLE IF EXISTS student_features;
CREATE EXTERNAL TABLE student_features (
  student_key STRING,
  school STRING,
  sex STRING,
  age INT,
  address STRING,
  studytime INT,
  failures INT,
  schoolsup STRING,
  famsup STRING,
  paid STRING,
  higher STRING,
  internet STRING,
  absences INT,
  g1 INT,
  g2 INT,
  g3 INT,
  grade_avg DOUBLE,
  grade_trend INT,
  absence_band STRING,
  studytime_band STRING,
  past_failure_flag BOOLEAN,
  support_count INT,
  course_avg_g3 DOUBLE,
  school_avg_g3 DOUBLE,
  course_absence_p75 DOUBLE,
  relative_to_course_avg DOUBLE,
  course_rank_by_g3 DOUBLE,
  course_row_count BIGINT,
  school_row_count BIGINT
)
PARTITIONED BY (course_id STRING)
STORED AS PARQUET
LOCATION '/data/student/mart/student_features';

DROP TABLE IF EXISTS risk_flags;
CREATE EXTERNAL TABLE risk_flags (
  student_key STRING,
  school STRING,
  sex STRING,
  age INT,
  studytime INT,
  failures INT,
  absences INT,
  g1 INT,
  g2 INT,
  g3 INT,
  grade_avg DOUBLE,
  grade_trend INT,
  absence_band STRING,
  studytime_band STRING,
  course_avg_g3 DOUBLE,
  school_avg_g3 DOUBLE,
  course_absence_p75 DOUBLE,
  relative_to_course_avg DOUBLE,
  risk_score INT,
  risk_level STRING,
  warning_reasons STRING,
  flag_low_final_grade BOOLEAN,
  flag_declining_grade BOOLEAN,
  flag_high_absences BOOLEAN,
  flag_previous_failures BOOLEAN,
  flag_low_studytime BOOLEAN
)
PARTITIONED BY (course_id STRING)
STORED AS PARQUET
LOCATION '/data/student/mart/risk_flags';

MSCK REPAIR TABLE student_features;
MSCK REPAIR TABLE risk_flags;

CREATE OR REPLACE VIEW v_at_risk_students AS
SELECT
  substr(r.student_key, 1, 12) AS student_id_masked,
  r.course_id,
  r.school,
  r.sex,
  r.age,
  r.risk_level,
  r.risk_score,
  r.warning_reasons,
  r.g1,
  r.g2,
  r.g3,
  r.grade_trend,
  r.absences,
  r.studytime,
  r.failures,
  r.relative_to_course_avg
FROM risk_flags r
WHERE r.risk_level IN ('MEDIUM', 'HIGH');

CREATE OR REPLACE VIEW v_course_risk_summary AS
SELECT
  course_id,
  COUNT(*) AS total_students,
  SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_students,
  SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_students,
  SUM(CASE WHEN risk_level IN ('MEDIUM', 'HIGH') THEN 1 ELSE 0 END) AS warned_students,
  ROUND(AVG(g3), 2) AS avg_final_grade,
  ROUND(AVG(absences), 2) AS avg_absences
FROM risk_flags
GROUP BY course_id;

CREATE OR REPLACE VIEW v_school_risk_summary AS
SELECT
  course_id,
  school,
  COUNT(*) AS total_students,
  SUM(CASE WHEN risk_level = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_students,
  SUM(CASE WHEN risk_level = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk_students,
  ROUND(AVG(g3), 2) AS avg_final_grade,
  ROUND(AVG(absences), 2) AS avg_absences
FROM risk_flags
GROUP BY course_id, school;
