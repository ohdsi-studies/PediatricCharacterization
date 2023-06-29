DROP TABLE IF EXISTS #covariate_result;
DROP TABLE IF EXISTS #person_ages;

CREATE TABLE #person_ages AS

SELECT
  cohort.cohort_definition_id,
  cohort.subject_id,
  FLOOR(YEAR(cohort.cohort_start_date) - person.year_of_birth) as age
FROM @cohort_table cohort
INNER JOIN @cdm_database_schema.person person
	ON cohort.subject_id = person.person_id
{@cohort_definition_id != -1} ? {
	WHERE cohort.cohort_definition_id IN (@cohort_definition_id)
};


CREATE TABLE #covariate_result AS
{@use_infant_age} ? {
SELECT
  CAST(200000000000 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age < 1
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(200000000001 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age < 1
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(200000000002 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age >= 2 AND age < 5
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(200000000003 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age >= 5 AND age < 11
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(200000000004 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age >= 11 AND age < 18
GROUP BY cohort_definition_id
}
{@use_infant_age_nsch} ? {
{@use_infant_age} ? {
UNION
}

SELECT
  CAST(100000000000 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age < 5
GROUP BY cohort_definition_id

UNION


SELECT
  CAST(100000000001 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value
FROM #person_ages
WHERE age > 5 AND age < 12
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(100000000002 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value

FROM #person_ages
WHERE age > 11 AND age < 15
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(100000000003 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value

FROM #person_ages
WHERE age > 15 AND age < 18
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(100000000004 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value

FROM #person_ages
WHERE age >= 1 AND age < 18
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(100000000005 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value

FROM #person_ages
WHERE age >= 3 AND age < 18
GROUP BY cohort_definition_id

UNION

SELECT
  CAST(100000000006 AS BIGINT) AS covariate_id,
  {@temporal} ? {CAST(NULL AS INT) AS time_id,}
	cohort_definition_id,
	COUNT(*) AS sum_value

FROM #person_ages
WHERE age >= 10 AND age < 18
GROUP BY cohort_definition_id;
}

TRUNCATE TABLE #person_ages;
DROP TABLE #person_ages;
