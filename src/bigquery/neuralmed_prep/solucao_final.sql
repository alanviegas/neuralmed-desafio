CREATE OR REPLACE TABLE `neuralmed_prep.solucao_final`
AS
WITH 
--identificando classificações duplicadas
dup as
(
SELECT scr.id, scr.label, scr.value, scr.score
FROM `neuralmed_prep.score` scr
INNER JOIN
    (select id, label, count(1) qtd
     from `neuralmed_prep.score` 
     group by 1,2
     having count(1)>1) dup 
  ON scr.id = dup.id  
  AND scr.label = dup.label
),
--identificando classificações não duplicadas
no_dup as 
(
SELECT scr.id, scr.label, scr.value, scr.score
FROM `neuralmed_prep.score` scr
LEFT OUTER JOIN
    (select id, label, count(1) qtd
     from `neuralmed_prep.score` 
     group by 1,2
     having count(1)>1) dup 
  ON scr.id = dup.id  
  AND scr.label = dup.label
WHERE dup.id is null
),
--calculando os scores e removendo os scores zerados dos duplicados
--e unindo os dois (duplicados com não-duplicados)
union_class as 
(
SELECT id, label, value, max(score)- min(score) as _score
FROM dup
GROUP BY id, label, value
HAVING max(score)- min(score)!=0
UNION ALL
SELECT id, label, value, score as _score
FROM no_dup
)
select * from union_class
