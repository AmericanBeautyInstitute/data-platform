MODEL (
  name staging.stg_google_sheets__students,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column enrolled_at
  ),
  grain student_id,
  cron '@daily',
  audits (assert_no_nulls(column := student_id))
);

SELECT
  CAST(student_id       AS STRING)  AS student_id,
  CAST(first_name       AS STRING)  AS first_name,
  CAST(last_name        AS STRING)  AS last_name,
  CAST(email            AS STRING)  AS email,
  CAST(phone            AS STRING)  AS phone,
  CAST(program_id       AS STRING)  AS program_id,
  CAST(enrollment_status AS STRING) AS enrollment_status,  -- active | graduated | withdrawn
  DATE(enrolled_at)                 AS enrolled_at,
  DATE(expected_grad_date)          AS expected_grad_date,
  DATE(actual_grad_date)            AS actual_grad_date,
  CAST(loaded_at        AS TIMESTAMP) AS loaded_at
FROM raw.google_sheets_students
WHERE
  enrolled_at BETWEEN @start_date AND @end_date
