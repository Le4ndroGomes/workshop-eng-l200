-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 03 — Silver: contas médicas (a tabela fato)
-- MAGIC
-- MAGIC Aqui está o dinheiro. `brz_conta_medica` tem ~120 mil guias e é a origem
-- MAGIC **mais suja** do workshop — como toda tabela transacional de verdade.
-- MAGIC
-- MAGIC Uma conta médica que entra na gold sem tratamento vira um número errado na
-- MAGIC mão do negócio. Então a silver da fato precisa responder três perguntas:
-- MAGIC
-- MAGIC 1. **Esta guia é única?** A origem reprocessa guias reenviadas → duplicatas.
-- MAGIC 2. **As chaves fecham?** Guia com prestador ou procedimento inexistente não
-- MAGIC    pode entrar em nenhuma soma.
-- MAGIC 3. **Os valores e datas fazem sentido?** Pago maior que apresentado, pago
-- MAGIC    negativo, apresentação antes do atendimento — tudo isso existe na origem.
-- MAGIC
-- MAGIC E o mais importante: **nada é descartado em silêncio**. Toda conta rejeitada
-- MAGIC vai para `qua_conta_invalida` com o motivo. A área de negócio precisa poder
-- MAGIC perguntar _"onde foram parar os R$ X que não estão no relatório?"_.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — diagnóstico: quantos defeitos existem?
-- MAGIC Antes de escrever regra, meça. Rode e veja o tamanho de cada problema.

-- COMMAND ----------

SELECT
  COUNT(*)                                                                    AS total_linhas,
  COUNT(*) - COUNT(DISTINCT NU_GUIA)                                          AS guias_duplicadas,
  SUM(CASE WHEN NU_PRESTADOR IS NULL THEN 1 ELSE 0 END)                       AS prestador_nulo,
  SUM(CASE WHEN CAST(FL_EXCLUIDO AS INT) = 1 THEN 1 ELSE 0 END)               AS contas_estornadas,
  SUM(CASE WHEN DT_APRESENTACAO < DT_ATENDIMENTO THEN 1 ELSE 0 END)           AS data_inconsistente,
  SUM(CASE WHEN VL_PAGO > VL_APRESENTADO THEN 1 ELSE 0 END)                   AS pago_maior_apresentado,
  SUM(CASE WHEN VL_PAGO < 0 THEN 1 ELSE 0 END)                                AS pago_negativo,
  SUM(CASE WHEN NU_AUTORIZACAO IS NULL THEN 1 ELSE 0 END)                     AS sem_autorizacao,
  SUM(CASE WHEN CAST(FL_ATEND_POS_DESCRED AS INT) = 1 THEN 1 ELSE 0 END)      AS atend_pos_descredenciamento
FROM IDENTIFIER(meu_schema || '.brz_conta_medica');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — deduplicação das guias
-- MAGIC
-- MAGIC ~2% das guias foram reenviadas pelo prestador e reprocessadas: **mesma
-- MAGIC `NU_GUIA`, duas linhas**, com `dt_carga_bronze` diferente e valores revisados.
-- MAGIC Somar as duas infla o custo. A regra de negócio é clara: vale a versão mais
-- MAGIC recente.
-- MAGIC
-- MAGIC Criamos uma **view temporária** com tipagem + exclusão + dedup, para os dois
-- MAGIC exercícios seguintes reaproveitarem a mesma base (sem repetir o SQL).
-- MAGIC
-- MAGIC ⚠️ View temporária vive na **sessão**, não no schema. Se o compute reiniciar,
-- MAGIC rode de novo esta célula (e a do `DECLARE`) antes dos exercícios.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_conta_dedup AS
SELECT
  CAST(NU_GUIA AS BIGINT)               AS NU_GUIA,
  CAST(NU_BENEFICIARIO AS BIGINT)       AS NU_BENEFICIARIO,
  CAST(NU_PRESTADOR AS BIGINT)          AS NU_PRESTADOR,
  CAST(CD_ESTABELECIMENTO AS BIGINT)    AS CD_ESTABELECIMENTO,
  CAST(CD_PROCEDIMENTO AS BIGINT)       AS CD_PROCEDIMENTO,
  CAST(CD_TIPO_ATENDIMENTO AS INT)      AS CD_TIPO_ATENDIMENTO,
  CAST(DT_ATENDIMENTO AS DATE)          AS DT_ATENDIMENTO,
  CAST(DT_APRESENTACAO AS DATE)         AS DT_APRESENTACAO,
  CAST(NU_COMPETENCIA AS INT)           AS NU_COMPETENCIA,
  CAST(QT_ITEM AS INT)                  AS QT_ITEM,
  CAST(VL_APRESENTADO AS DECIMAL(18,2)) AS VL_APRESENTADO,
  CAST(VL_GLOSA AS DECIMAL(18,2))       AS VL_GLOSA,
  CAST(VL_PAGO AS DECIMAL(18,2))        AS VL_PAGO,
  NU_AUTORIZACAO,
  CAST(FL_ATEND_POS_DESCRED AS INT)     AS FL_ATEND_POS_DESCRED
FROM IDENTIFIER(meu_schema || '.brz_conta_medica')
WHERE CAST(FL_EXCLUIDO AS INT) = 0                      -- QUALIDADE: conta estornada não conta
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY NU_GUIA ORDER BY dt_carga_bronze DESC
) = 1;                                                  -- QUALIDADE: 1 linha por guia

-- COMMAND ----------

SELECT COUNT(*) AS linhas_apos_dedup FROM vw_conta_dedup;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 (com o Assistant) — a fato limpa
-- MAGIC
-- MAGIC Agora as camadas de **integridade referencial** e **regras de negócio**.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela IDENTIFIER(meu_schema || '.slv_conta_medica') a partir da view
-- MAGIC > vw_conta_dedup, com INNER JOIN em
-- MAGIC > IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento') por NU_PRESTADOR,
-- MAGIC > INNER JOIN em IDENTIFIER(meu_schema || '.brz_procedimento') por
-- MAGIC > CD_PROCEDIMENTO e INNER JOIN em
-- MAGIC > IDENTIFIER(meu_schema || '.slv_beneficiario_plano') por NU_BENEFICIARIO.
-- MAGIC > Filtre apenas as contas onde DT_APRESENTACAO >= DT_ATENDIMENTO,
-- MAGIC > VL_PAGO >= 0, VL_PAGO <= VL_APRESENTADO e o CD_ESTABELECIMENTO da conta é
-- MAGIC > igual ao CD_ESTABELECIMENTO do cadastro do prestador. Traga as colunas da
-- MAGIC > conta mais VL_REFERENCIA, CD_GRUPO_PROCEDIMENTO, NM_GRUPO_PROCEDIMENTO e
-- MAGIC > FL_ALTA_COMPLEXIDADE do procedimento, com CAST de VL_REFERENCIA para
-- MAGIC > DECIMAL(18,2)."_
-- MAGIC
-- MAGIC 💡 `VL_REFERENCIA` (valor de tabela do procedimento) é a coluna que vai
-- MAGIC permitir, no módulo 05, comparar **o que o prestador cobrou** com **o que o
-- MAGIC procedimento vale**. Sem ela não existe detecção de anomalia.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 (com o Assistant) — quarentena com motivo
-- MAGIC
-- MAGIC Onde foi parar o que o exercício 1 rejeitou? Agora registramos, **com o motivo
-- MAGIC e com o valor**, cada conta que não entrou na fato.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela IDENTIFIER(meu_schema || '.qua_conta_invalida') a partir de
-- MAGIC > vw_conta_dedup usando LEFT JOIN com slv_prestador_estabelecimento,
-- MAGIC > brz_procedimento e slv_beneficiario_plano. Crie uma coluna
-- MAGIC > motivo_quarentena com um CASE que retorna 'PRESTADOR_NULO' quando
-- MAGIC > NU_PRESTADOR é nulo, 'PRESTADOR_INEXISTENTE' quando o prestador não foi
-- MAGIC > encontrado, 'PROCEDIMENTO_INEXISTENTE' quando o procedimento não foi
-- MAGIC > encontrado, 'BENEFICIARIO_INEXISTENTE' quando o beneficiário não foi
-- MAGIC > encontrado, 'ESTABELECIMENTO_DIVERGENTE' quando o estabelecimento da conta
-- MAGIC > difere do cadastro do prestador, 'DATA_APRESENTACAO_ANTERIOR' quando
-- MAGIC > DT_APRESENTACAO < DT_ATENDIMENTO, 'VALOR_PAGO_NEGATIVO' quando VL_PAGO < 0 e
-- MAGIC > 'PAGO_MAIOR_QUE_APRESENTADO' quando VL_PAGO > VL_APRESENTADO. Mantenha apenas
-- MAGIC > as linhas em que o motivo não é nulo, e traga NU_GUIA, NU_PRESTADOR,
-- MAGIC > NU_COMPETENCIA, VL_PAGO, motivo_quarentena e CURRENT_TIMESTAMP() como
-- MAGIC > dt_quarentena."_

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência
-- MAGIC `validas + em_quarentena` deve fechar com `linhas_apos_dedup`.

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM vw_conta_dedup)                                    AS apos_dedup,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_conta_medica'))     AS validas,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.qua_conta_invalida'))   AS em_quarentena;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — quanto dinheiro ficou de fora, e por quê

-- COMMAND ----------

SELECT
  motivo_quarentena,
  COUNT(*)                           AS contas,
  ROUND(SUM(VL_PAGO), 2)             AS valor_retido
FROM IDENTIFIER(meu_schema || '.qua_conta_invalida')
GROUP BY ALL
ORDER BY contas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Por que `NU_AUTORIZACAO` nulo (~6% das guias) **não** manda a conta para a
-- MAGIC    quarentena?
-- MAGIC 2. A ordem importa: se você tivesse aplicado a integridade referencial
-- MAGIC    **antes** da deduplicação, o resultado mudaria?
-- MAGIC 3. `FL_ATEND_POS_DESCRED = 1` indica atendimento feito depois do
-- MAGIC    descredenciamento do prestador. Isso é erro de dado ou achado de negócio?
