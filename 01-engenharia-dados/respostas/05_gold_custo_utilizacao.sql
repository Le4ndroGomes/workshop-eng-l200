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
-- MAGIC # Módulo 05 — Gold: custo e utilização por prestador (GABARITO)
-- MAGIC
-- MAGIC Grão: **prestador × competência**. Agregação + as-of join com a dimensão
-- MAGIC histórica + métricas de negócio + indicador de anomalia.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Agregação no grão

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW vw_conta_agregada AS
SELECT
  NU_PRESTADOR,
  NU_COMPETENCIA,
  TO_DATE(CAST(NU_COMPETENCIA AS STRING), 'yyyyMM') AS DT_COMPETENCIA,
  COUNT(*)                                         AS QT_CONTAS,
  COUNT(DISTINCT NU_BENEFICIARIO)                  AS QT_BENEFICIARIOS,
  SUM(VL_PAGO)                                     AS VL_CUSTO_TOTAL,
  SUM(VL_APRESENTADO)                              AS VL_APRESENTADO_TOTAL,
  SUM(VL_GLOSA)                                    AS VL_GLOSA_TOTAL,
  SUM(VL_REFERENCIA * QT_ITEM)                     AS VL_REFERENCIA_TOTAL,
  SUM(FL_ATEND_POS_DESCRED)                        AS QT_CONTAS_POS_DESCREDENCIAMENTO
FROM IDENTIFIER(meu_schema || '.slv_conta_medica')
GROUP BY ALL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício — `gold_custo_utilizacao_prestador`

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador') AS
WITH com_vigencia AS (
  -- AS-OF JOIN: a competência escolhe a vigência que estava valendo naquela data
  SELECT
    a.NU_PRESTADOR, a.NU_COMPETENCIA, a.DT_COMPETENCIA,
    a.QT_CONTAS, a.QT_BENEFICIARIOS,
    a.VL_CUSTO_TOTAL, a.VL_APRESENTADO_TOTAL, a.VL_GLOSA_TOTAL, a.VL_REFERENCIA_TOTAL,
    a.QT_CONTAS_POS_DESCREDENCIAMENTO,
    v.CD_ESPECIALIDADE,
    v.FL_STATUS_PRESTADOR,
    v.NU_CAPACIDADE_ATEND_MES
  FROM vw_conta_agregada a
  INNER JOIN IDENTIFIER(meu_schema || '.slv_prestador_vigencia') v
          ON v.NU_PRESTADOR     = a.NU_PRESTADOR
         AND a.DT_COMPETENCIA  >= v.DT_INICIO_VIGENCIA
         AND a.DT_COMPETENCIA   < v.DT_FIM_VIGENCIA         -- fim exclusivo
  -- rede de segurança: garante o grão mesmo se houvesse sobreposição
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.NU_PRESTADOR, a.NU_COMPETENCIA
    ORDER BY v.DT_INICIO_VIGENCIA DESC
  ) = 1
),
com_dimensao AS (
  SELECT
    c.*,
    d.NM_PRESTADOR, d.CD_TIPO_PRESTADOR,
    d.CD_ESTABELECIMENTO, d.NM_ESTABELECIMENTO, d.SG_UF, d.NM_REGIAO
  FROM com_vigencia c
  INNER JOIN IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento') d
          ON d.NU_PRESTADOR = c.NU_PRESTADOR
),
metricas AS (
  SELECT
    *,
    ROUND(VL_CUSTO_TOTAL / NULLIF(QT_CONTAS, 0), 2)                        AS VL_CUSTO_MEDIO_CONTA,
    ROUND(VL_CUSTO_TOTAL / NULLIF(QT_BENEFICIARIOS, 0), 2)                 AS VL_CUSTO_MEDIO_BENEFICIARIO,
    ROUND(100 * VL_GLOSA_TOTAL / NULLIF(VL_APRESENTADO_TOTAL, 0), 2)       AS PCT_GLOSA,
    ROUND(100.0 * QT_CONTAS / NULLIF(NU_CAPACIDADE_ATEND_MES, 0), 2)       AS TX_UTILIZACAO_CAPACIDADE,
    -- métrica insensível ao mix de procedimentos
    ROUND(VL_CUSTO_TOTAL / NULLIF(VL_REFERENCIA_TOTAL, 0), 4)              AS IDX_CUSTO_TABELA
  FROM com_dimensao
)
SELECT
  XXHASH64(CAST(NU_PRESTADOR AS STRING), CAST(NU_COMPETENCIA AS STRING)) AS SK_CUSTO_PRESTADOR_MES,
  NU_PRESTADOR, NM_PRESTADOR, NU_COMPETENCIA, DT_COMPETENCIA,
  CD_ESPECIALIDADE,
  CASE CD_ESPECIALIDADE
    WHEN 1 THEN 'Clinica Medica'          WHEN 2 THEN 'Cardiologia'
    WHEN 3 THEN 'Ortopedia'               WHEN 4 THEN 'Pediatria'
    WHEN 5 THEN 'Ginecologia/Obstetricia' WHEN 6 THEN 'Oncologia'
    WHEN 7 THEN 'Diagnostico por Imagem'  WHEN 8 THEN 'Analises Clinicas'
    ELSE 'Nao informado' END                                              AS NM_ESPECIALIDADE,
  CD_TIPO_PRESTADOR, FL_STATUS_PRESTADOR,
  CD_ESTABELECIMENTO, NM_ESTABELECIMENTO, SG_UF, NM_REGIAO,
  QT_CONTAS, QT_BENEFICIARIOS, QT_CONTAS_POS_DESCREDENCIAMENTO,
  NU_CAPACIDADE_ATEND_MES, TX_UTILIZACAO_CAPACIDADE,
  VL_CUSTO_TOTAL, VL_APRESENTADO_TOTAL, VL_GLOSA_TOTAL, VL_REFERENCIA_TOTAL,
  VL_CUSTO_MEDIO_CONTA, VL_CUSTO_MEDIO_BENEFICIARIO, PCT_GLOSA,
  IDX_CUSTO_TABELA,
  -- normalização contra os pares: mesma especialidade, mesma competência
  ROUND(IDX_CUSTO_TABELA / NULLIF(
    AVG(IDX_CUSTO_TABELA) OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA), 0), 4
  )                                                                       AS IDX_CUSTO_VS_PARES,
  CASE WHEN IDX_CUSTO_TABELA / NULLIF(
              AVG(IDX_CUSTO_TABELA) OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA), 0
            ) >= 1.5
            AND QT_CONTAS >= 10                                           -- piso de volume
       THEN 1 ELSE 0 END                                                  AS FL_ANOMALIA_CUSTO
FROM metricas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferências

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM vw_conta_agregada)                                           AS pares_prestador_competencia,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')) AS linhas_gold;

-- COMMAND ----------

SELECT
  COUNT(*)                                          AS linhas,
  COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES)            AS sks_distintas,
  COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) AS duplicadas
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador');

-- COMMAND ----------

SELECT
  NU_COMPETENCIA,
  COUNT(*)                                AS prestadores_com_conta,
  SUM(FL_ANOMALIA_CUSTO)                  AS prestadores_sinalizados,
  ROUND(SUM(VL_CUSTO_TOTAL) / 1000000, 2) AS custo_milhoes
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. Porque **plano e faixa etária não pertencem a este grão**. Um prestador
-- MAGIC    atende beneficiários de vários planos no mesmo mês; colocar "plano" aqui
-- MAGIC    exigiria escolher um arbitrariamente (errado) ou quebrar o grão (mudando a
-- MAGIC    tabela). Análise por plano é outra gold — ou uma consulta que junta
-- MAGIC    `slv_conta_medica` com `slv_beneficiario_plano`, como no módulo 08.
-- MAGIC    Gold enxuta e com grão declarado > gold "que tem tudo".
-- MAGIC 2. Todas as competências receberiam a especialidade e o status **de hoje**.
-- MAGIC    Prestadores que mudaram de especialidade teriam todo o histórico de custo
-- MAGIC    realocado, distorcendo a média dos pares — e, com ela, a detecção de
-- MAGIC    anomalia. Seria um erro invisível: nenhuma query falha, só o número mente.
-- MAGIC 3. A média dos pares sobe junto e o índice relativo cai: a anomalia "se
-- MAGIC    esconde na multidão". É exatamente o que acontece com o aumento regional
-- MAGIC    plantado nos dados — ele não é pego pelo `FL_ANOMALIA_CUSTO` e precisa ser
-- MAGIC    encontrado por agregação de região × competência (desafio final, módulo 08).
-- MAGIC    Uma técnica não cobre todo tipo de anomalia.
