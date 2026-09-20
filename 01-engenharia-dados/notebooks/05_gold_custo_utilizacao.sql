-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 05 — Gold: custo e utilização por prestador
-- MAGIC
-- MAGIC Este é o entregável do workshop: a tabela que a área de Gestão de Rede pediu.
-- MAGIC
-- MAGIC **Grão:** uma linha por **prestador × competência**. Definir o grão antes de
-- MAGIC escrever SQL é a decisão mais importante de uma tabela gold — é ela que
-- MAGIC determina o que pode e o que não pode ser somado.
-- MAGIC
-- MAGIC **Três coisas acontecem aqui:**
-- MAGIC
-- MAGIC 1. **Agregação** da fato no grão escolhido (custo, contas, beneficiários).
-- MAGIC 2. **As-of join** com a dimensão histórica do módulo 04: cada competência
-- MAGIC    recebe os atributos que valiam **naquela** data, não os de hoje.
-- MAGIC 3. **Métricas de negócio**, incluindo o indicador de anomalia de custo.
-- MAGIC
-- MAGIC ## Como medir "custo fora do padrão" sem se enganar
-- MAGIC
-- MAGIC O reflexo é usar **custo médio por conta**. É uma métrica ruim aqui: um
-- MAGIC prestador que fez duas cirurgias no mês tem custo médio altíssimo sem nada de
-- MAGIC errado. O ruído do *mix* de procedimentos esconde o sinal.
-- MAGIC
-- MAGIC A métrica que usamos é o **índice de custo sobre tabela de referência**:
-- MAGIC
-- MAGIC ```
-- MAGIC IDX_CUSTO_TABELA = SUM(VL_PAGO) / SUM(VL_REFERENCIA * QT_ITEM)
-- MAGIC ```
-- MAGIC
-- MAGIC Ela compara o que foi pago com o que aqueles procedimentos **valem em tabela**.
-- MAGIC Assim o mix é neutralizado: cirurgia e consulta entram cada uma pelo seu valor
-- MAGIC de referência. Depois normalizamos contra os **pares** (mesma especialidade, mesma
-- MAGIC competência) para responder _"caro comparado a quem faz o mesmo tipo de coisa"_.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — a agregação no grão prestador × competência
-- MAGIC Parte mecânica, já resolvida. Note `TO_DATE` convertendo a competência
-- MAGIC numérica (`202503`) na data do primeiro dia do mês — é essa data que o as-of
-- MAGIC join usa para escolher a vigência.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_conta_agregada AS
SELECT
  NU_PRESTADOR,
  NU_COMPETENCIA,
  TO_DATE(CAST(NU_COMPETENCIA AS STRING), 'yyyyMM')     AS DT_COMPETENCIA,
  COUNT(*)                                              AS QT_CONTAS,
  COUNT(DISTINCT NU_BENEFICIARIO)                       AS QT_BENEFICIARIOS,
  SUM(VL_PAGO)                                          AS VL_CUSTO_TOTAL,
  SUM(VL_APRESENTADO)                                   AS VL_APRESENTADO_TOTAL,
  SUM(VL_GLOSA)                                         AS VL_GLOSA_TOTAL,
  SUM(VL_REFERENCIA * QT_ITEM)                          AS VL_REFERENCIA_TOTAL,
  SUM(FL_ATEND_POS_DESCRED)                             AS QT_CONTAS_POS_DESCREDENCIAMENTO
FROM IDENTIFIER(meu_schema || '.slv_conta_medica')
GROUP BY ALL;

-- COMMAND ----------

SELECT * FROM vw_conta_agregada ORDER BY VL_CUSTO_TOTAL DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — o as-of join, explicado
-- MAGIC
-- MAGIC Um join normal casa chave com chave. Um **as-of join** casa chave **e tempo**:
-- MAGIC
-- MAGIC ```sql
-- MAGIC ON  v.NU_PRESTADOR    =  a.NU_PRESTADOR
-- MAGIC AND a.DT_COMPETENCIA >=  v.DT_INICIO_VIGENCIA
-- MAGIC AND a.DT_COMPETENCIA <   v.DT_FIM_VIGENCIA     -- fim exclusivo!
-- MAGIC ```
-- MAGIC
-- MAGIC O `<` no fim (e não `<=`) é o que evita a conta cair em duas vigências
-- MAGIC adjacentes ao mesmo tempo. Mesmo assim mantemos um `QUALIFY ROW_NUMBER()` como
-- MAGIC rede de segurança: **o grão da gold não pode depender da sorte**.
-- MAGIC
-- MAGIC Rode a célula abaixo para ver um prestador com mais de uma vigência e entender
-- MAGIC o que o join precisa escolher.

-- COMMAND ----------

SELECT NU_PRESTADOR, CD_ESPECIALIDADE, FL_STATUS_PRESTADOR,
       DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA
FROM IDENTIFIER(meu_schema || '.slv_prestador_vigencia')
QUALIFY COUNT(*) OVER (PARTITION BY NU_PRESTADOR) >= 3
ORDER BY NU_PRESTADOR, DT_INICIO_VIGENCIA
LIMIT 12;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício-chave (com o Assistant) — a tabela gold
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant** (peça em partes se preferir):
-- MAGIC > _"Crie a tabela IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
-- MAGIC > em três CTEs._
-- MAGIC >
-- MAGIC > _CTE 1 (com_vigencia): junte a view vw_conta_agregada com
-- MAGIC > IDENTIFIER(meu_schema || '.slv_prestador_vigencia') usando INNER JOIN por
-- MAGIC > NU_PRESTADOR, com DT_COMPETENCIA maior ou igual a DT_INICIO_VIGENCIA e menor
-- MAGIC > que DT_FIM_VIGENCIA; mantenha uma linha por prestador e competência com
-- MAGIC > QUALIFY ROW_NUMBER() OVER (PARTITION BY NU_PRESTADOR, NU_COMPETENCIA ORDER BY
-- MAGIC > DT_INICIO_VIGENCIA DESC) = 1. Traga da vigência CD_ESPECIALIDADE,
-- MAGIC > FL_STATUS_PRESTADOR e NU_CAPACIDADE_ATEND_MES._
-- MAGIC >
-- MAGIC > _CTE 2 (com_dimensao): junte a CTE 1 com
-- MAGIC > IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento') por NU_PRESTADOR
-- MAGIC > para trazer NM_PRESTADOR, CD_TIPO_PRESTADOR, CD_ESTABELECIMENTO,
-- MAGIC > NM_ESTABELECIMENTO, SG_UF e NM_REGIAO._
-- MAGIC >
-- MAGIC > _CTE 3 (metricas): calcule VL_CUSTO_MEDIO_CONTA = VL_CUSTO_TOTAL / QT_CONTAS,
-- MAGIC > VL_CUSTO_MEDIO_BENEFICIARIO = VL_CUSTO_TOTAL / QT_BENEFICIARIOS,
-- MAGIC > PCT_GLOSA = 100 * VL_GLOSA_TOTAL / VL_APRESENTADO_TOTAL,
-- MAGIC > TX_UTILIZACAO_CAPACIDADE = 100 * QT_CONTAS / NU_CAPACIDADE_ATEND_MES e
-- MAGIC > IDX_CUSTO_TABELA = VL_CUSTO_TOTAL / VL_REFERENCIA_TOTAL, usando NULLIF nos
-- MAGIC > denominadores e ROUND com 4 casas._
-- MAGIC >
-- MAGIC > _No SELECT final: crie SK_CUSTO_PRESTADOR_MES com
-- MAGIC > XXHASH64(CAST(NU_PRESTADOR AS STRING), CAST(NU_COMPETENCIA AS STRING)),
-- MAGIC > traduza CD_ESPECIALIDADE em NM_ESPECIALIDADE com um CASE (1 Clinica Medica,
-- MAGIC > 2 Cardiologia, 3 Ortopedia, 4 Pediatria, 5 Ginecologia/Obstetricia,
-- MAGIC > 6 Oncologia, 7 Diagnostico por Imagem, 8 Analises Clinicas), calcule
-- MAGIC > IDX_CUSTO_VS_PARES = IDX_CUSTO_TABELA dividido por
-- MAGIC > AVG(IDX_CUSTO_TABELA) OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA) e
-- MAGIC > crie FL_ANOMALIA_CUSTO = 1 quando IDX_CUSTO_VS_PARES >= 1.5 e QT_CONTAS >= 10,
-- MAGIC > senão 0."_
-- MAGIC
-- MAGIC 💡 O `QT_CONTAS >= 10` não é capricho: com poucas contas, o índice oscila
-- MAGIC muito e qualquer limiar gera falso positivo. Toda regra de anomalia precisa de
-- MAGIC um piso de volume.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 1 — o as-of join não pode perder linhas
-- MAGIC Os dois números devem ser **iguais**. Se a gold tiver menos linhas, existe
-- MAGIC competência sem vigência correspondente (buraco na dimensão histórica).

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM vw_conta_agregada)                                              AS pares_prestador_competencia,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador'))    AS linhas_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 2 — a chave surrogada é única
-- MAGIC `duplicadas` deve ser **zero**. Uma SK repetida significa que o grão da gold
-- MAGIC não é o que você declarou.

-- COMMAND ----------

SELECT
  COUNT(*)                                     AS linhas,
  COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES)       AS sks_distintas,
  COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) AS duplicadas
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 3 — a gold já responde a pergunta do negócio?

-- COMMAND ----------

SELECT
  NU_COMPETENCIA,
  COUNT(*)                                  AS prestadores_com_conta,
  SUM(FL_ANOMALIA_CUSTO)                    AS prestadores_sinalizados,
  ROUND(SUM(VL_CUSTO_TOTAL) / 1000000, 2)   AS custo_milhoes
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Por que a gold **não** tem colunas de plano nem de faixa etária?
-- MAGIC 2. O que mudaria no resultado se o join com a vigência fosse feito com
-- MAGIC    `slv_prestador_estabelecimento` (estado atual) em vez da tabela de vigências?
-- MAGIC 3. `IDX_CUSTO_VS_PARES` usa `AVG` dos pares. Se um grupo tiver **muitos**
-- MAGIC    prestadores anômalos, o que acontece com a detecção?
