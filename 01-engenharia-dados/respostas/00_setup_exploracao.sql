-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 00 — Contexto de Negócio e Exploração (GABARITO)
-- MAGIC
-- MAGIC > ⚠️ Todos os dados utilizados no workshop são sintéticos e não representam
-- MAGIC > pacientes reais.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Volume das origens

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_estabelecimento')) AS estabelecimentos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador'))       AS prestadores,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_au_prestador'))       AS eventos_auditoria,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_plano'))           AS planos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_beneficiario'))    AS beneficiarios,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sia_tb_procedimento'))    AS procedimentos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica'))    AS contas_medicas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Custo por competência (o sintoma)

-- COMMAND ----------

SELECT
  CAST(NU_COMPETENCIA AS INT)      AS competencia,
  COUNT(*)                         AS contas,
  ROUND(SUM(VL_PAGO) / 1000000, 2) AS custo_milhoes
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica')
GROUP BY ALL
ORDER BY competencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício — órfãos de integridade referencial
-- MAGIC
-- MAGIC Três checagens de chave, uma por relacionamento crítico.

-- COMMAND ----------

-- Prestadores sem estabelecimento correspondente
SELECT COUNT(*) AS prestadores_orfaos
FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador') p
LEFT JOIN IDENTIFIER(meu_schema || '.raw_sgr_tb_estabelecimento') e
       ON e.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO
WHERE e.CD_ESTABELECIMENTO IS NULL;

-- COMMAND ----------

-- Contas médicas sem prestador correspondente (inclui prestador nulo)
SELECT
  COUNT(*)                                                       AS contas_sem_prestador_valido,
  SUM(CASE WHEN c.NU_PRESTADOR IS NULL THEN 1 ELSE 0 END)         AS prestador_nulo,
  SUM(CASE WHEN c.NU_PRESTADOR IS NOT NULL THEN 1 ELSE 0 END)     AS prestador_inexistente
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica') c
LEFT JOIN IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador') p
       ON p.NU_PRESTADOR = c.NU_PRESTADOR
WHERE p.NU_PRESTADOR IS NULL;

-- COMMAND ----------

-- Contas médicas com código de procedimento que não existe no catálogo
SELECT COUNT(*) AS contas_procedimento_invalido
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica') c
LEFT JOIN IDENTIFIER(meu_schema || '.raw_sia_tb_procedimento') pr
       ON pr.CD_PROCEDIMENTO = c.CD_PROCEDIMENTO
WHERE pr.CD_PROCEDIMENTO IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. **Fato:** `raw_sia_tb_conta_medica` (um evento por guia, com métricas de
-- MAGIC    valor e quantidade). **Dimensões:** prestador, estabelecimento,
-- MAGIC    beneficiário, plano e procedimento.
-- MAGIC 2. O custo sobe a partir de **2025-10** e segue alto em 11 e 12.
-- MAGIC 3. Descartar órfãos em silêncio esconde erro de origem: o custo "desaparece"
-- MAGIC    do relatório sem ninguém saber. Por isso eles vão para **quarentena**,
-- MAGIC    com motivo registrado, e entram no painel de qualidade (módulo 06).
