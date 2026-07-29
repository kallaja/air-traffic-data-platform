-- Validate silver.dim_country (SCD Type 2)

-- 1) Current row count (≈ number of countries in source, ~250)
SELECT COUNT(*) AS current_row_count
FROM silver.dim_country
WHERE is_current = true;

-- 2) Exactly one current row per country
SELECT
  COUNT(*) AS current_rows,
  COUNT(DISTINCT country_cca2) AS distinct_current_cca2
FROM silver.dim_country
WHERE is_current = true;
-- Expect: equal

-- 3) Required fields on current rows
SELECT
  SUM(CASE WHEN country_cca2 IS NULL THEN 1 ELSE 0 END) AS null_cca2,
  SUM(CASE WHEN name_common IS NULL THEN 1 ELSE 0 END) AS null_name_common,
  SUM(CASE WHEN row_hash IS NULL THEN 1 ELSE 0 END) AS null_row_hash
FROM silver.dim_country
WHERE is_current = true;
-- Expect: all 0

-- 4) SCD2 integrity
SELECT
  SUM(CASE WHEN is_current = true AND valid_to <> DATE('9999-12-31') THEN 1 ELSE 0 END) AS bad_current_valid_to,
  SUM(CASE WHEN is_current = false AND valid_to = DATE('9999-12-31') THEN 1 ELSE 0 END) AS bad_closed_valid_to,
  SUM(CASE WHEN valid_from >= valid_to THEN 1 ELSE 0 END) AS bad_date_window
FROM silver.dim_country;
-- Expect: all 0

-- 5) History depth (after 2+ loads with changes, closed_rows > 0)
SELECT
  SUM(CASE WHEN is_current = true THEN 1 ELSE 0 END) AS current_rows,
  SUM(CASE WHEN is_current = false THEN 1 ELSE 0 END) AS closed_rows,
  COUNT(*) AS total_versions
FROM silver.dim_country;

-- 6) As-of example: country version effective on a given date
SELECT
  country_cca2,
  name_common,
  population,
  valid_from,
  valid_to,
  is_current
FROM silver.dim_country
WHERE country_cca2 = 'PL'
  AND DATE '2026-07-29' >= valid_from
  AND DATE '2026-07-29' < valid_to;

-- 7) Sample current Europe
SELECT
  country_cca2,
  name_common,
  capital,
  population,
  valid_from,
  is_current
FROM silver.dim_country
WHERE is_current = true
  AND region = 'Europe'
ORDER BY population DESC NULLS LAST
LIMIT 30;
