MODEL (
  name staging.stg_google_sheets__programs,
  kind FULL,
  grain program_id,
  cron '@daily',
  audits (assert_no_nulls(column := program_id))
);

SELECT
  CAST(program_id          AS STRING)  AS program_id,
  CAST(program_name        AS STRING)  AS program_name,
  CAST(program_code        AS STRING)  AS program_code,
  CAST(duration_weeks      AS INT64)   AS duration_weeks,
  CAST(max_enrollment      AS INT64)   AS max_enrollment,
  CAST(is_active           AS BOOL)    AS is_active,
  CAST(loaded_at           AS TIMESTAMP) AS loaded_at
FROM raw.google_sheets_programs
