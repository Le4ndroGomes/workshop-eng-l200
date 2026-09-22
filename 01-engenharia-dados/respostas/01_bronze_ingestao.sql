-- Databricks notebook source
-- MAGIC %md
-- MAGIC <img src="../../assets/images/db-academy.png" alt="Databricks Academy" width="320"/>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <div style="
-- MAGIC   border-left: 4px solid #1976d2;
-- MAGIC   background: #e3f2fd;
-- MAGIC   padding: 14px 18px;
-- MAGIC   border-radius: 4px;
-- MAGIC   margin: 16px 0;
-- MAGIC ">
-- MAGIC <div style="color:#333;">
-- MAGIC
-- MAGIC #### Informações do ambiente
-- MAGIC
-- MAGIC - Você trabalha em um <strong>schema pessoal</strong> dentro do catálogo <strong>amil_workshop_trilha_tech</strong>, derivado do seu usuário (ex.: <strong>maria_silva</strong>).
-- MAGIC
-- MAGIC - O schema e os dados sintéticos são criados pelo notebook <strong>00-setup/setup_participantes.py</strong>, que deve ser executado <strong>uma única vez</strong> antes dos módulos.
-- MAGIC
-- MAGIC - São necessárias permissões de <strong>CREATE SCHEMA</strong> e <strong>CREATE TABLE</strong> no catálogo. Sem permissão de <strong>CREATE CATALOG</strong>, aponte a variável <code>CATALOG</code> do setup para um catálogo que você já tenha.
-- MAGIC
-- MAGIC - Rode os módulos <strong>em ordem</strong>: cada um lê as tabelas criadas pelo anterior.
-- MAGIC
-- MAGIC - <strong>Todos os dados utilizados no workshop são sintéticos e não representam pacientes, prestadores ou contratos reais.</strong>
-- MAGIC
-- MAGIC </div>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Módulo 01 — Bronze: Ingestão das 7 origens (GABARITO)

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SGR — rede credenciada

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_estabelecimento') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_estabelecimento');

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_prestador') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador');

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_prestador_auditoria') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sgr_au_prestador');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SGB — carteira e planos

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_plano') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_plano');

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_beneficiario') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_beneficiario');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SIA — assistencial

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_procedimento') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_procedimento');

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.brz_conta_medica') AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_estabelecimento'))     AS brz_estabelecimento,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_prestador'))           AS brz_prestador,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_prestador_auditoria')) AS brz_prestador_auditoria,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_plano'))               AS brz_plano,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_beneficiario'))        AS brz_beneficiario,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_procedimento'))        AS brz_procedimento,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_conta_medica'))        AS brz_conta_medica;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. Porque o bronze é **evidência**, não verdade. Se ele já filtrasse contas
-- MAGIC    estornadas, não haveria como auditar o que a origem enviou nem como mudar
-- MAGIC    a regra depois sem reingerir. A regra de negócio mora na silver.
-- MAGIC 2. `dt_carga_bronze` é a data que **a origem afirma**; `_dt_ingestao` é a data
-- MAGIC    em que **o lakehouse leu** o dado. As duas juntas permitem medir latência
-- MAGIC    de origem e reconstruir qualquer carga passada.
