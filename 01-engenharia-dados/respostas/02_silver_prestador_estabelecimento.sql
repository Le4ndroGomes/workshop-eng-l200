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
-- MAGIC # Módulo 02 — Silver: dimensões conformadas (GABARITO)
-- MAGIC
-- MAGIC Produz `slv_prestador_estabelecimento`, `qua_prestador_orfao` e
-- MAGIC `slv_beneficiario_plano`, aplicando as 4 camadas de qualidade:
-- MAGIC tipagem, filtro de exclusão, deduplicação e integridade referencial.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensão 1 — prestador + estabelecimento

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento') AS
WITH prestador_limpo AS (
  SELECT
    CAST(NU_PRESTADOR AS BIGINT)                    AS NU_PRESTADOR,
    CAST(CD_ESTABELECIMENTO AS BIGINT)              AS CD_ESTABELECIMENTO,
    CD_CNES,
    NM_PRESTADOR,
    CAST(FL_STATUS_PRESTADOR AS INT)                AS FL_STATUS_PRESTADOR,
    CAST(CD_ESPECIALIDADE AS INT)                   AS CD_ESPECIALIDADE,
    CAST(CD_TIPO_PRESTADOR AS INT)                  AS CD_TIPO_PRESTADOR,
    CAST(NU_UNIDADE AS INT)                         AS NU_UNIDADE,
    CAST(DT_CREDENCIAMENTO AS DATE)                 AS DT_CREDENCIAMENTO,
    CAST(DT_DESCREDENCIAMENTO AS DATE)              AS DT_DESCREDENCIAMENTO,
    CAST(CD_MOTIVO_DESCREDENCIAMENTO AS INT)        AS CD_MOTIVO_DESCREDENCIAMENTO,
    CAST(NU_CAPACIDADE_ATEND_MES AS INT)            AS NU_CAPACIDADE_ATEND_MES
  FROM IDENTIFIER(meu_schema || '.brz_prestador')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0                          -- QUALIDADE 2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NU_PRESTADOR ORDER BY dt_carga_bronze DESC
  ) = 1                                                       -- QUALIDADE 3
),
estabelecimento_limpo AS (
  SELECT
    CAST(CD_ESTABELECIMENTO AS BIGINT)      AS CD_ESTABELECIMENTO,
    NM_ESTABELECIMENTO,
    CAST(CD_TIPO_ESTABELECIMENTO AS INT)    AS CD_TIPO_ESTABELECIMENTO,
    SG_UF,
    NM_MUNICIPIO,
    NM_REGIAO,
    CAST(DT_INICIO_OPERACAO AS DATE)        AS DT_INICIO_OPERACAO,
    CAST(NU_LEITOS AS INT)                  AS NU_LEITOS,
    CAST(NU_CNPJ AS DECIMAL(38,0))          AS NU_CNPJ
  FROM IDENTIFIER(meu_schema || '.brz_estabelecimento')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CD_ESTABELECIMENTO ORDER BY dt_carga_bronze DESC
  ) = 1
)
SELECT
  p.NU_PRESTADOR, p.CD_CNES, p.NM_PRESTADOR,
  p.FL_STATUS_PRESTADOR, p.CD_ESPECIALIDADE,
  CASE p.CD_ESPECIALIDADE
    WHEN 1 THEN 'Clinica Medica'        WHEN 2 THEN 'Cardiologia'
    WHEN 3 THEN 'Ortopedia'             WHEN 4 THEN 'Pediatria'
    WHEN 5 THEN 'Ginecologia/Obstetricia' WHEN 6 THEN 'Oncologia'
    WHEN 7 THEN 'Diagnostico por Imagem' WHEN 8 THEN 'Analises Clinicas'
    ELSE 'Nao informado' END                                   AS NM_ESPECIALIDADE,
  p.CD_TIPO_PRESTADOR, p.NU_UNIDADE,
  p.DT_CREDENCIAMENTO, p.DT_DESCREDENCIAMENTO, p.CD_MOTIVO_DESCREDENCIAMENTO,
  p.NU_CAPACIDADE_ATEND_MES,
  e.CD_ESTABELECIMENTO, e.NM_ESTABELECIMENTO, e.CD_TIPO_ESTABELECIMENTO,
  e.SG_UF, e.NM_MUNICIPIO, e.NM_REGIAO, e.DT_INICIO_OPERACAO, e.NU_LEITOS, e.NU_CNPJ
FROM prestador_limpo p
INNER JOIN estabelecimento_limpo e                            -- QUALIDADE 4
        ON e.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 — quarentena dos prestadores órfãos
-- MAGIC Registro que não fecha chave não é descartado: fica registrado com o motivo.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.qua_prestador_orfao') AS
SELECT
  CAST(p.NU_PRESTADOR AS BIGINT)       AS NU_PRESTADOR,
  CAST(p.CD_ESTABELECIMENTO AS BIGINT) AS CD_ESTABELECIMENTO,
  p.CD_CNES,
  'CD_ESTABELECIMENTO sem correspondencia em brz_estabelecimento' AS motivo_quarentena,
  CURRENT_TIMESTAMP()                  AS dt_quarentena
FROM IDENTIFIER(meu_schema || '.brz_prestador') p
LEFT JOIN IDENTIFIER(meu_schema || '.brz_estabelecimento') e
       ON e.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO
WHERE CAST(p.FL_EXCLUIDO AS INT) = 0
  AND e.CD_ESTABELECIMENTO IS NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 — dimensão beneficiário + plano
-- MAGIC Mesmo padrão, outras tabelas. Sem PII: nenhuma coluna de nome, CPF ou endereço.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.slv_beneficiario_plano') AS
WITH beneficiario_limpo AS (
  SELECT
    CAST(NU_BENEFICIARIO AS BIGINT)   AS NU_BENEFICIARIO,
    NU_CARTEIRA,
    CAST(CD_PLANO AS INT)             AS CD_PLANO,
    SG_UF,
    NM_REGIAO,
    CAST(NU_FAIXA_ETARIA AS INT)      AS NU_FAIXA_ETARIA,
    CD_SEXO,
    CAST(DT_ADESAO AS DATE)           AS DT_ADESAO,
    CAST(DT_CANCELAMENTO AS DATE)     AS DT_CANCELAMENTO,
    CAST(FL_ATIVO AS INT)             AS FL_ATIVO
  FROM IDENTIFIER(meu_schema || '.brz_beneficiario')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NU_BENEFICIARIO ORDER BY dt_carga_bronze DESC
  ) = 1
),
plano_limpo AS (
  SELECT
    CAST(CD_PLANO AS INT)                      AS CD_PLANO,
    NM_PLANO,
    CAST(CD_SEGMENTACAO AS INT)                AS CD_SEGMENTACAO,
    CASE CAST(CD_SEGMENTACAO AS INT)
      WHEN 1 THEN 'Ambulatorial'
      WHEN 2 THEN 'Hospitalar'
      WHEN 3 THEN 'Ambulatorial + Hospitalar'
      ELSE 'Nao informado' END                 AS NM_SEGMENTACAO,
    NM_ACOMODACAO,
    CAST(FL_COPARTICIPACAO AS INT)             AS FL_COPARTICIPACAO,
    CAST(VL_MENSALIDADE_BASE AS DECIMAL(18,2)) AS VL_MENSALIDADE_BASE
  FROM IDENTIFIER(meu_schema || '.brz_plano')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CD_PLANO ORDER BY dt_carga_bronze DESC
  ) = 1
)
SELECT
  b.NU_BENEFICIARIO, b.NU_CARTEIRA, b.SG_UF, b.NM_REGIAO,
  b.NU_FAIXA_ETARIA, b.CD_SEXO, b.DT_ADESAO, b.DT_CANCELAMENTO, b.FL_ATIVO,
  p.CD_PLANO, p.NM_PLANO, p.CD_SEGMENTACAO, p.NM_SEGMENTACAO,
  p.NM_ACOMODACAO, p.FL_COPARTICIPACAO, p.VL_MENSALIDADE_BASE
FROM beneficiario_limpo b
INNER JOIN plano_limpo p ON p.CD_PLANO = b.CD_PLANO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento')) AS prestadores_validos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.qua_prestador_orfao'))           AS prestadores_em_quarentena,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_beneficiario_plano'))        AS beneficiarios_validos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_prestador')
    WHERE CAST(FL_EXCLUIDO AS INT) = 0)                                             AS prestadores_nao_excluidos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. `DISTINCT` **não** resolveria: as duas versões do estabelecimento diferem em
-- MAGIC    `NM_ESTABELECIMENTO`, `NU_LEITOS` e `dt_carga_bronze`, então as duas linhas
-- MAGIC    são distintas e ambas sobreviveriam — duplicando o prestador no join e
-- MAGIC    **dobrando o custo** na gold. Dedup exige escolher uma versão por chave, e
-- MAGIC    isso é `ROW_NUMBER()` com um critério de ordenação explícito.
-- MAGIC 2. Porque um mesmo código traduzido em cinco relatórios diferentes vira cinco
-- MAGIC    verdades diferentes. A silver é o lugar de definir o domínio uma única vez.
-- MAGIC 3. Contando nulos por coluna (`COUNT(*) - COUNT(coluna)`) antes de usar.
-- MAGIC    Coluna 100% nula na origem é uma armadilha comum: o join "funciona" e
-- MAGIC    devolve zero linha.
