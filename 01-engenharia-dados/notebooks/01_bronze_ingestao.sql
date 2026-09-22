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
-- MAGIC # Módulo 01 — Bronze: Ingestão das 7 origens
-- MAGIC
-- MAGIC No setup você gerou as tabelas de **landing** (`raw_sgr_*`, `raw_sgb_*`,
-- MAGIC `raw_sia_*`) no seu próprio schema. Agora materializamos a camada **bronze**
-- MAGIC (`brz_*`), que é a cópia fiel da origem **mais metadados de ingestão**.
-- MAGIC
-- MAGIC **Regra de ouro do bronze:** não corrigir nada. Tipos continuam crus, valores
-- MAGIC errados continuam errados. O bronze responde _"o que a origem me mandou e
-- MAGIC quando"_ — se alguém questionar um número três meses depois, é aqui que se
-- MAGIC prova o que chegou.
-- MAGIC
-- MAGIC Conceitos:
-- MAGIC - `CREATE OR REPLACE TABLE ... AS SELECT` (CTAS) para materializar dados;
-- MAGIC - coluna técnica de ingestão (`_dt_ingestao`) para rastreabilidade;
-- MAGIC - idempotência: reexecutar o notebook não duplica nada.

-- COMMAND ----------

-- 👇 TROQUE `seu_usuario` pelo nome do schema que o setup criou para você.
--    É o seu e-mail antes do @, com o ponto trocado por underscore.
--    Ex.: maria.silva@amil.com.br  ->  maria_silva
USE CATALOG amil_workshop_trilha_tech;
USE SCHEMA seu_usuario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Células prontas — rede credenciada (SGR)

-- COMMAND ----------

CREATE OR REPLACE TABLE brz_estabelecimento AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM raw_sgr_tb_estabelecimento;

-- COMMAND ----------

CREATE OR REPLACE TABLE brz_prestador AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM raw_sgr_tb_prestador;

-- COMMAND ----------

CREATE OR REPLACE TABLE brz_prestador_auditoria AS
SELECT *, CURRENT_TIMESTAMP() AS _dt_ingestao
FROM raw_sgr_au_prestador;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício-chave (com o Assistant) — ingerir as 4 origens restantes
-- MAGIC
-- MAGIC Faltam as origens de **carteira** (SGB) e **assistencial** (SIA). O padrão é
-- MAGIC sempre o mesmo — repita-o para as quatro tabelas.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Seguindo exatamente o padrão da célula acima (CREATE OR REPLACE TABLE
-- MAGIC > com SELECT * e CURRENT_TIMESTAMP() AS _dt_ingestao), crie as tabelas bronze:
-- MAGIC > brz_plano a partir de raw_sgb_tb_plano,
-- MAGIC > brz_beneficiario a partir de raw_sgb_tb_beneficiario,
-- MAGIC > brz_procedimento a partir de raw_sia_tb_procedimento e
-- MAGIC > brz_conta_medica a partir de raw_sia_tb_conta_medica."_
-- MAGIC
-- MAGIC ⚠️ Use **exatamente** esses nomes de tabela bronze: os módulos seguintes
-- MAGIC dependem deles.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência
-- MAGIC As contagens do bronze devem ser **idênticas** às do landing (módulo 00).
-- MAGIC Qualquer diferença aqui significa que você transformou algo que não devia.

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM brz_estabelecimento)     AS brz_estabelecimento,
  (SELECT COUNT(*) FROM brz_prestador)           AS brz_prestador,
  (SELECT COUNT(*) FROM brz_prestador_auditoria) AS brz_prestador_auditoria,
  (SELECT COUNT(*) FROM brz_plano)               AS brz_plano,
  (SELECT COUNT(*) FROM brz_beneficiario)        AS brz_beneficiario,
  (SELECT COUNT(*) FROM brz_procedimento)        AS brz_procedimento,
  (SELECT COUNT(*) FROM brz_conta_medica)        AS brz_conta_medica;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Por que o bronze **não** filtra `FL_EXCLUIDO = 1`?
-- MAGIC 2. O que `_dt_ingestao` resolve que `dt_carga_bronze` (coluna da origem) não resolve?
