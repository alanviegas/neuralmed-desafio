# Neuralmed - Desafio para engenharia de dados

Aqui na Neuralmed trabalhamos com grande quantidade de dados relativos a exames e 
laudos médicos. Esses dados precisam ser organizados em um `datalake`.

Você receberá em anexo o arquivo `data.zip`. Dentro perceberá a seguinte estrutura:
- exam
- medical_report

Precisamos alimentar essas tabelas do datalake.

---
**Exam**:

|  column       |   type  |
|---------------|---------|
| id            |  string |
| source        |  string |
| dataset_type  |  string |
| dataset       |  string |
| body_part     |  string |
| image_path    |  string |

---
**Label**:

|    column            |    type  |
|----------------------|----------|
| exam_id              |  string  |
| classification       |  string  |
| labelled_by          |  string  |
| labelled_date        |  date    |
| labelling_method     |  string  |
| classification_type  |  string  |
| value                |  boolean |

---
**Medical Report**:
|  column      |   type  |
|--------------|---------|
| exam_id      |  string |
| report_date  |  date   |
| reported_by  |  string |
| text         |  string |
---

Tarefas:

1. Crie um script/job usando preferencialmente pyspark (também podendo usar python ou sql) para ler os dados desse .zip e carregar nas tabelas acima.
2. Crie o código para responder as seguintes perguntas (podendo ser em SQL, python ou pyspark, o que considerar mais otimizado):
   
   2.1) Dado que o número de classificações de label que temos é 5, defina a % de completude, e retorne a quantidade de exames que temos com menos de 25%

      Amostra do cálculo de completude:

      | id      |  labels                                               |  completude |
      |---------|-------------------------------------------------------|-------------|
      |  1      |  ["covid19", "edema", "tumor", "hernia", "mass"]      |   100%      |
      |  2      |  []                                                   |   0%        |
      |  3      |  ["covid19"]                                          |   20%       |

   2.2) Calcular os labels agregados, unindo classificações duplicadas baseadas em seu score, considerando:
      - Labels de physician valem 10
      - Labels de ml_model valem 5
   
   Quando o score zerar (nem False nem True "vencem"), remover o label do resultado.
      
   Precisamos de um resultado com id do exame, label, value e seu score.

   Passo a passo (somente para clareza do problema):
   1. Defina o score:

   
      | id      |  label         |   labelling_method   |   value    |   score |
      |---------|----------------|----------------------|------------|---------|
      |  1      |  "covid19"     |   ml_model           |   True     |       5 |
      |  1      |  "covid19"     |   physician          |   True     |      10 |
      |  1      |  "covid19"     |   ml_model           |   False    |       5 |
      |  2      |  "covid19"     |   ml_model           |   False    |       5 |
      |  2      |  "hernia"      |   physician          |   True     |      10 |
      |  2      |  "hernia"      |   physician          |   False    |      10 |
      |  2      |  "mass"        |   physician          |   False    |      10 |
      |  2      |  "mass"        |   ml_model           |   True     |       5 |


   2. Agregue os labels (esse é o resultado desejado)

      | id      |  label         |   value    |   score |
      |---------|----------------|------------|---------|
      |  1      |  "covid19"     |   True     |      10 |
      |  2      |  "covid19"     |   False    |       5 |
      |  2      |  "mass"        |   False    |       5 |

Importante: 
- Mantenha o código claro e bem documentado (README);
- Valorizamos bastante a criação de testes unitários / de integração;
- Você pode fazer o desafio em um repositório github privado e dar permissão para gente ou mande um .zip;
- De preferência, use docker para que possamos executar sem precisar seguir um passo a passo de instalação;
- Não queremos uma solução que envolva uma arquitetura complexa, acessar um banco de dados remoto ou algo em cloud. Explique a solução realizada e porque foi feito dessa maneira.
- Vamos avaliar a performance e escalabilidade, considerando que essa quantidade de dados pode crescer rapidamente.
