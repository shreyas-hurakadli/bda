USE student_warn;

-- 1. Dataset proof: expected total is 1044 rows after ingest.
SELECT
  COUNT(*) AS total_student_course_rows,
  COUNT(DISTINCT course_id) AS courses
FROM student_course_performance;

-- 2. Course distribution: Math and Portuguese should both appear.
SELECT
  course_id,
  COUNT(*) AS rows_per_course,
  ROUND(AVG(g3), 2) AS avg_final_grade,
  ROUND(AVG(absences), 2) AS avg_absences
FROM student_course_performance
GROUP BY course_id
ORDER BY course_id;

-- 3. Main warning report for reviewers.
SELECT
  student_id_masked,
  course_id,
  school,
  risk_level,
  risk_score,
  warning_reasons,
  g1,
  g2,
  g3,
  grade_trend,
  absences,
  studytime,
  failures
FROM v_at_risk_students
ORDER BY risk_score DESC, g3 ASC, absences DESC
LIMIT 25;

-- 4. Course-level warning summary.
SELECT
  course_id,
  total_students,
  high_risk_students,
  medium_risk_students,
  warned_students,
  avg_final_grade,
  avg_absences
FROM v_course_risk_summary
ORDER BY warned_students DESC;

-- 5. School-level warning summary.
SELECT
  course_id,
  school,
  total_students,
  high_risk_students,
  medium_risk_students,
  avg_final_grade,
  avg_absences
FROM v_school_risk_summary
ORDER BY course_id, school;

-- 6. Explainable rule contribution counts.
SELECT
  course_id,
  SUM(CASE WHEN flag_low_final_grade THEN 1 ELSE 0 END) AS low_final_grade_count,
  SUM(CASE WHEN flag_declining_grade THEN 1 ELSE 0 END) AS declining_grade_count,
  SUM(CASE WHEN flag_high_absences THEN 1 ELSE 0 END) AS high_absences_count,
  SUM(CASE WHEN flag_previous_failures THEN 1 ELSE 0 END) AS previous_failures_count,
  SUM(CASE WHEN flag_low_studytime THEN 1 ELSE 0 END) AS low_studytime_count
FROM risk_flags
GROUP BY course_id
ORDER BY course_id;
