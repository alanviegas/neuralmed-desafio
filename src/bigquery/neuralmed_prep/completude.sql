CREATE OR REPLACE TABLE `neuralmed_prep.completude`
AS
WITH raw as (
SELECT DISTINCT 
  id, 
  classification
FROM `neuralmed_raw.Label`
)
SELECT id,
  ARRAY_AGG(STRUCT(classification)) as labels,
  CASE ARRAY_LENGTH(ARRAY_AGG(DISTINCT classification))
    WHEN 1 THEN 20
    WHEN 2 THEN 40
    WHEN 3 THEN 60
    WHEN 4 THEN 80
    WHEN 5 THEN 100
    ELSE 0
    END AS completude
FROM raw
GROUP BY id