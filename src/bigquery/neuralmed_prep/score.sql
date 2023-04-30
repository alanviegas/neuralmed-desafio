CREATE OR REPLACE TABLE `neuralmed_prep.score`
AS
-- tabela com o cálculo de score
SELECT 
  id, 
  classification as label, 
  labelling_method, 
  value, 
  if(labelling_method="ml_model",5,10) as score 
FROM `neuralmed_raw.Label`
WHERE if(labelling_method="ml_model",5,10) != 0 
