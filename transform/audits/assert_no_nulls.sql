AUDIT (
  name assert_no_nulls,
  dialect bigquery
);
SELECT *
FROM @this_model
WHERE @column IS NULL
