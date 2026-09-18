-- External table over curated UCI student performance Parquet
USE student_warn;

DROP TABLE IF EXISTS student_course_performance;
CREATE EXTERNAL TABLE student_course_performance (
  school STRING,
  sex STRING,
  address STRING,
  famsize STRING,
  pstatus STRING,
  mjob STRING,
  fjob STRING,
  reason STRING,
  guardian STRING,
  schoolsup STRING,
  famsup STRING,
  paid STRING,
  activities STRING,
  nursery STRING,
  higher STRING,
  internet STRING,
  romantic STRING,
  age INT,
  medu INT,
  fedu INT,
  traveltime INT,
  studytime INT,
  failures INT,
  famrel INT,
  freetime INT,
  goout INT,
  dalc INT,
  walc INT,
  health INT,
  absences INT,
  g1 INT,
  g2 INT,
  g3 INT,
  source_file STRING,
  student_key STRING
)
PARTITIONED BY (course_id STRING)
STORED AS PARQUET
LOCATION '/data/student/curated/student_course_performance';

MSCK REPAIR TABLE student_course_performance;
