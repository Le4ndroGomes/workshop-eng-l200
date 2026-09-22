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
-- MAGIC # Módulo 03 — Silver: contas médicas (GABARITO)
-- MAGIC
-- MAGIC Produz `slv_conta_medica` (fato limpa) e `qua_conta_invalida` (quarentena com
-- MAGIC motivo), a partir de `brz_conta_medica`.

-- COMMAND ----------

-- 👇 TROQUE `seu_usuario` pelo nome do schema que o setup criou para você.
--    É o seu e-mail antes do @, com o ponto trocado por underscore.
--    Ex.: maria.silva@amil.com.br  ->  maria_silva
USE CATALOG amil_workshop_trilha_tech;
USE SCHEMA seu_usuario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Diagnóstico dos defeitos

-- COMMAND ----------

SELECT
  COUNT(*)                                                               AS total_linhas,
  COUNT(*) - COUNT(DISTINCT NU_GUIA)                                     AS guias_duplicadas,
  SUM(CASE WHEN NU_PRESTADOR IS NULL THEN 1 ELSE 0 END)                  AS prestador_nulo,
  SUM(CASE WHEN CAST(FL_EXCLUIDO AS INT) = 1 THEN 1 ELSE 0 END)          AS contas_estornadas,
  SUM(CASE WHEN DT_APRESENTACAO < DT_ATENDIMENTO THEN 1 ELSE 0 END)      AS data_inconsistente,
  SUM(CASE WHEN VL_PAGO > VL_APRESENTADO THEN 1 ELSE 0 END)              AS pago_maior_apresentado,
  SUM(CASE WHEN VL_PAGO < 0 THEN 1 ELSE 0 END)                           AS pago_negativo,
  SUM(CASE WHEN NU_AUTORIZACAO IS NULL THEN 1 ELSE 0 END)                AS sem_autorizacao,
  SUM(CASE WHEN CAST(FL_ATEND_POS_DESCRED AS INT) = 1 THEN 1 ELSE 0 END) AS atend_pos_descredenciamento
FROM brz_conta_medica;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Base deduplicada (tipagem + exclusão + dedup)

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
FROM brz_conta_medica
WHERE CAST(FL_EXCLUIDO AS INT) = 0
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY NU_GUIA ORDER BY dt_carga_bronze DESC
) = 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 — `slv_conta_medica`
-- MAGIC
-- MAGIC Integridade referencial (3 INNER JOINs) + regras de valor e data.
-- MAGIC O join com `slv_prestador_estabelecimento` já elimina, de graça, as contas de
-- MAGIC prestadores órfãos ou excluídos — porque essa dimensão **já** foi tratada.

-- COMMAND ----------

CREATE OR REPLACE TABLE slv_conta_medica AS
SELECT
  c.NU_GUIA,
  c.NU_COMPETENCIA,
  c.DT_ATENDIMENTO,
  c.DT_APRESENTACAO,
  c.NU_BENEFICIARIO,
  c.NU_PRESTADOR,
  c.CD_ESTABELECIMENTO,
  c.CD_PROCEDIMENTO,
  CAST(pr.CD_GRUPO_PROCEDIMENTO AS INT)       AS CD_GRUPO_PROCEDIMENTO,
  pr.NM_GRUPO_PROCEDIMENTO,
  CAST(pr.FL_ALTA_COMPLEXIDADE AS INT)        AS FL_ALTA_COMPLEXIDADE,
  c.CD_TIPO_ATENDIMENTO,
  CASE c.CD_TIPO_ATENDIMENTO
    WHEN 1 THEN 'Ambulatorial' WHEN 2 THEN 'Internacao'
    WHEN 3 THEN 'Urgencia/Emergencia' WHEN 4 THEN 'Exame/SADT'
    ELSE 'Nao informado' END                  AS NM_TIPO_ATENDIMENTO,
  c.QT_ITEM,
  CAST(pr.VL_REFERENCIA AS DECIMAL(18,2))     AS VL_REFERENCIA,
  c.VL_APRESENTADO,
  c.VL_GLOSA,
  c.VL_PAGO,
  c.NU_AUTORIZACAO,
  c.FL_ATEND_POS_DESCRED
FROM vw_conta_dedup c
INNER JOIN slv_prestador_estabelecimento p   -- RI: prestador
        ON p.NU_PRESTADOR = c.NU_PRESTADOR
INNER JOIN brz_procedimento pr              -- RI: procedimento
        ON CAST(pr.CD_PROCEDIMENTO AS BIGINT) = c.CD_PROCEDIMENTO
INNER JOIN slv_beneficiario_plano b         -- RI: beneficiário
        ON b.NU_BENEFICIARIO = c.NU_BENEFICIARIO
WHERE c.DT_APRESENTACAO >= c.DT_ATENDIMENTO                              -- regra de data
  AND c.VL_PAGO >= 0                                                     -- regra de valor
  AND c.VL_PAGO <= c.VL_APRESENTADO                                      -- regra de valor
  AND c.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO;                       -- regra de consistência

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 — `qua_conta_invalida`
-- MAGIC
-- MAGIC O `CASE` é avaliado em ordem: cada conta recebe o **primeiro** motivo que se
-- MAGIC aplica. Guardamos `VL_PAGO` para saber quanto dinheiro está retido.

-- COMMAND ----------

CREATE OR REPLACE TABLE qua_conta_invalida AS
WITH avaliada AS (
  SELECT
    c.NU_GUIA, c.NU_PRESTADOR, c.NU_COMPETENCIA, c.VL_PAGO,
    CASE
      WHEN c.NU_PRESTADOR IS NULL                        THEN 'PRESTADOR_NULO'
      WHEN p.NU_PRESTADOR IS NULL                        THEN 'PRESTADOR_INEXISTENTE'
      WHEN pr.CD_PROCEDIMENTO IS NULL                    THEN 'PROCEDIMENTO_INEXISTENTE'
      WHEN b.NU_BENEFICIARIO IS NULL                     THEN 'BENEFICIARIO_INEXISTENTE'
      WHEN c.CD_ESTABELECIMENTO <> p.CD_ESTABELECIMENTO  THEN 'ESTABELECIMENTO_DIVERGENTE'
      WHEN c.DT_APRESENTACAO < c.DT_ATENDIMENTO          THEN 'DATA_APRESENTACAO_ANTERIOR'
      WHEN c.VL_PAGO < 0                                 THEN 'VALOR_PAGO_NEGATIVO'
      WHEN c.VL_PAGO > c.VL_APRESENTADO                  THEN 'PAGO_MAIOR_QUE_APRESENTADO'
    END AS motivo_quarentena
  FROM vw_conta_dedup c
  LEFT JOIN slv_prestador_estabelecimento p
         ON p.NU_PRESTADOR = c.NU_PRESTADOR
  LEFT JOIN brz_procedimento pr
         ON CAST(pr.CD_PROCEDIMENTO AS BIGINT) = c.CD_PROCEDIMENTO
  LEFT JOIN slv_beneficiario_plano b
         ON b.NU_BENEFICIARIO = c.NU_BENEFICIARIO
)
SELECT
  NU_GUIA, NU_PRESTADOR, NU_COMPETENCIA, VL_PAGO, motivo_quarentena,
  CURRENT_TIMESTAMP() AS dt_quarentena
FROM avaliada
WHERE motivo_quarentena IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM vw_conta_dedup)                                  AS apos_dedup,
  (SELECT COUNT(*) FROM slv_conta_medica)   AS validas,
  (SELECT COUNT(*) FROM qua_conta_invalida) AS em_quarentena;

-- COMMAND ----------

SELECT
  motivo_quarentena,
  COUNT(*)               AS contas,
  ROUND(SUM(VL_PAGO), 2) AS valor_retido
FROM qua_conta_invalida
GROUP BY ALL
ORDER BY contas DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. Porque autorização ausente **não invalida o custo**: a conta foi paga e o
-- MAGIC    dinheiro saiu. É um problema de **processo** (guia liberada sem autorização
-- MAGIC    prévia), não de integridade do dado. Vira métrica informativa no painel do
-- MAGIC    módulo 06. Regra prática: só vai para quarentena o que tornaria a soma errada.
-- MAGIC 2. Sim, e para pior. Se a integridade viesse antes, uma guia duplicada com
-- MAGIC    prestador inválido na versão antiga e válido na nova (ou vice-versa) poderia
-- MAGIC    sobreviver nas duas pontas — entrando na fato **e** na quarentena.
-- MAGIC    Deduplicar primeiro garante uma decisão por guia.
-- MAGIC 3. Nem um nem outro sozinho: é um **achado de negócio detectado por dado**.
-- MAGIC    O dado está correto (o atendimento aconteceu); o que está errado é a
-- MAGIC    operação. Por isso ele fica na fato, sinalizado, e sobe como alerta no
-- MAGIC    painel de qualidade em vez de ser apagado.
