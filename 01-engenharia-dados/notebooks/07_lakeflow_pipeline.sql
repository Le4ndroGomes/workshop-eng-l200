-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 07 — Lakeflow Declarative Pipeline (SQL)
-- MAGIC
-- MAGIC Aqui reescrevemos o pipeline dos módulos 02–05 de forma **declarativa**.
-- MAGIC Em vez de dizer *como* fazer (um `CREATE TABLE` depois do outro, na ordem
-- MAGIC certa), declaramos *o que cada tabela é*; o Lakeflow deduz a ordem, materializa
-- MAGIC e aplica as regras de qualidade — as mesmas que escrevemos "na mão" antes.
-- MAGIC
-- MAGIC > **Este módulo não tem exercícios.** O notebook é o **código-fonte de um
-- MAGIC > Pipeline** — ele roda como um todo, não célula a célula. Leia, entenda e
-- MAGIC > execute-o criando um Pipeline (instruções no fim).
-- MAGIC
-- MAGIC ## Conceitos-chave
-- MAGIC - `CREATE OR REFRESH MATERIALIZED VIEW` — tabela gerenciada pelo pipeline.
-- MAGIC - Prefixo `LIVE.` — referência a **outra tabela do mesmo pipeline**; é isso que
-- MAGIC   cria as dependências que o Lakeflow monta automaticamente no grafo. As tabelas
-- MAGIC   de origem (`raw_sgr_*`, `raw_sgb_*`, `raw_sia_*`, geradas no setup) são
-- MAGIC   referenciadas **sem** `LIVE.` e **sem qualificação**: o Lakeflow as resolve no
-- MAGIC   schema-destino do pipeline (o **seu** schema, definido na criação).
-- MAGIC - `CONSTRAINT ... EXPECT (...) [ON VIOLATION DROP ROW]` — regras de qualidade
-- MAGIC   declarativas, com métricas visíveis no painel do pipeline. É o equivalente
-- MAGIC   gerenciado do painel `dq_metricas` que construímos no módulo 06.
-- MAGIC
-- MAGIC ## O que muda em relação ao imperativo
-- MAGIC | | Imperativo (módulos 02–05) | Declarativo (este módulo) |
-- MAGIC |---|---|---|
-- MAGIC | Ordem de execução | você garante | o Lakeflow deduz pelo `LIVE.` |
-- MAGIC | Regras de qualidade | `WHERE` / tabelas `qua_*` | `CONSTRAINT ... EXPECT` |
-- MAGIC | Visibilidade | você escreve o painel | painel de expectativas pronto |
-- MAGIC | Linhas rejeitadas | ficam na quarentena, auditáveis | são **descartadas** (contadas, não guardadas) |
-- MAGIC
-- MAGIC ⚠️ Essa última linha é a diferença que importa no nosso caso de uso: para
-- MAGIC responder *"quanto dinheiro está preso por conta inválida?"* a quarentena
-- MAGIC explícita continua necessária. Declarativo não substitui desenho de dados.
-- MAGIC
-- MAGIC > **Prefixo `sdp_`:** um Lakeflow Pipeline precisa ser **dono** das tabelas que
-- MAGIC > materializa. Se as tabelas `slv_*`/`gold_*` já existirem (você as criou nos
-- MAGIC > módulos 02–05), o pipeline falha com `TABLE_ALREADY_EXISTS`. Por isso as
-- MAGIC > tabelas daqui usam o prefixo **`sdp_`**: a versão declarativa **convive** com
-- MAGIC > a imperativa no mesmo schema, e você pode comparar os resultados.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver 1 — dimensão prestador + estabelecimento
-- MAGIC Tipagem, exclusão, deduplicação e integridade referencial — exatamente o
-- MAGIC módulo 02, agora com as regras declaradas no cabeçalho.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_slv_prestador_estabelecimento (
  CONSTRAINT estabelecimento_valido EXPECT (CD_ESTABELECIMENTO IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT status_conhecido       EXPECT (FL_STATUS_PRESTADOR IS NOT NULL),
  CONSTRAINT capacidade_plausivel   EXPECT (NU_CAPACIDADE_ATEND_MES BETWEEN 1 AND 5000)
) AS
WITH prestador_limpo AS (
  SELECT
    CAST(NU_PRESTADOR AS BIGINT)             AS NU_PRESTADOR,
    CAST(CD_ESTABELECIMENTO AS BIGINT)       AS CD_ESTABELECIMENTO,
    CD_CNES,
    NM_PRESTADOR,
    CAST(FL_STATUS_PRESTADOR AS INT)         AS FL_STATUS_PRESTADOR,
    CAST(CD_ESPECIALIDADE AS INT)            AS CD_ESPECIALIDADE,
    CAST(CD_TIPO_PRESTADOR AS INT)           AS CD_TIPO_PRESTADOR,
    CAST(NU_UNIDADE AS INT)                  AS NU_UNIDADE,
    CAST(DT_CREDENCIAMENTO AS DATE)          AS DT_CREDENCIAMENTO,
    CAST(DT_DESCREDENCIAMENTO AS DATE)       AS DT_DESCREDENCIAMENTO,
    CAST(CD_MOTIVO_DESCREDENCIAMENTO AS INT) AS CD_MOTIVO_DESCREDENCIAMENTO,
    CAST(NU_CAPACIDADE_ATEND_MES AS INT)     AS NU_CAPACIDADE_ATEND_MES
  FROM raw_sgr_tb_prestador
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY NU_PRESTADOR ORDER BY dt_carga_bronze DESC) = 1
),
estabelecimento_limpo AS (
  SELECT
    CAST(CD_ESTABELECIMENTO AS BIGINT)   AS CD_ESTABELECIMENTO,
    NM_ESTABELECIMENTO,
    CAST(CD_TIPO_ESTABELECIMENTO AS INT) AS CD_TIPO_ESTABELECIMENTO,
    SG_UF, NM_MUNICIPIO, NM_REGIAO,
    CAST(DT_INICIO_OPERACAO AS DATE)     AS DT_INICIO_OPERACAO,
    CAST(NU_LEITOS AS INT)               AS NU_LEITOS,
    CAST(NU_CNPJ AS DECIMAL(38,0))       AS NU_CNPJ
  FROM raw_sgr_tb_estabelecimento
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CD_ESTABELECIMENTO ORDER BY dt_carga_bronze DESC) = 1
)
SELECT
  p.NU_PRESTADOR, p.CD_CNES, p.NM_PRESTADOR,
  p.FL_STATUS_PRESTADOR, p.CD_ESPECIALIDADE, p.CD_TIPO_PRESTADOR, p.NU_UNIDADE,
  p.DT_CREDENCIAMENTO, p.DT_DESCREDENCIAMENTO, p.CD_MOTIVO_DESCREDENCIAMENTO,
  p.NU_CAPACIDADE_ATEND_MES,
  e.CD_ESTABELECIMENTO, e.NM_ESTABELECIMENTO, e.CD_TIPO_ESTABELECIMENTO,
  e.SG_UF, e.NM_MUNICIPIO, e.NM_REGIAO, e.DT_INICIO_OPERACAO, e.NU_LEITOS, e.NU_CNPJ
FROM prestador_limpo p
INNER JOIN estabelecimento_limpo e ON e.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver 2 — dimensão beneficiário + plano

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_slv_beneficiario_plano (
  CONSTRAINT plano_valido       EXPECT (CD_PLANO IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT faixa_etaria_valida EXPECT (NU_FAIXA_ETARIA BETWEEN 1 AND 10)
) AS
WITH beneficiario_limpo AS (
  SELECT
    CAST(NU_BENEFICIARIO AS BIGINT) AS NU_BENEFICIARIO,
    NU_CARTEIRA,
    CAST(CD_PLANO AS INT)           AS CD_PLANO,
    SG_UF, NM_REGIAO,
    CAST(NU_FAIXA_ETARIA AS INT)    AS NU_FAIXA_ETARIA,
    CD_SEXO,
    CAST(DT_ADESAO AS DATE)         AS DT_ADESAO,
    CAST(DT_CANCELAMENTO AS DATE)   AS DT_CANCELAMENTO,
    CAST(FL_ATIVO AS INT)           AS FL_ATIVO
  FROM raw_sgb_tb_beneficiario
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY NU_BENEFICIARIO ORDER BY dt_carga_bronze DESC) = 1
),
plano_limpo AS (
  SELECT
    CAST(CD_PLANO AS INT)                      AS CD_PLANO,
    NM_PLANO,
    CAST(CD_SEGMENTACAO AS INT)                AS CD_SEGMENTACAO,
    NM_ACOMODACAO,
    CAST(FL_COPARTICIPACAO AS INT)             AS FL_COPARTICIPACAO,
    CAST(VL_MENSALIDADE_BASE AS DECIMAL(18,2)) AS VL_MENSALIDADE_BASE
  FROM raw_sgb_tb_plano
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CD_PLANO ORDER BY dt_carga_bronze DESC) = 1
)
SELECT
  b.NU_BENEFICIARIO, b.NU_CARTEIRA, b.SG_UF, b.NM_REGIAO,
  b.NU_FAIXA_ETARIA, b.CD_SEXO, b.DT_ADESAO, b.DT_CANCELAMENTO, b.FL_ATIVO,
  p.CD_PLANO, p.NM_PLANO, p.CD_SEGMENTACAO, p.NM_ACOMODACAO,
  p.FL_COPARTICIPACAO, p.VL_MENSALIDADE_BASE
FROM beneficiario_limpo b
INNER JOIN plano_limpo p ON p.CD_PLANO = b.CD_PLANO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver 3 — fato conta médica
-- MAGIC Aqui as regras de negócio que no módulo 03 eram `WHERE` viram `CONSTRAINT`.
-- MAGIC Note que as duas referências `LIVE.` fazem o Lakeflow esperar as duas dimensões.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_slv_conta_medica (
  CONSTRAINT data_coerente      EXPECT (DT_APRESENTACAO >= DT_ATENDIMENTO)  ON VIOLATION DROP ROW,
  CONSTRAINT valor_nao_negativo EXPECT (VL_PAGO >= 0)                       ON VIOLATION DROP ROW,
  CONSTRAINT pago_ate_apresentado EXPECT (VL_PAGO <= VL_APRESENTADO)        ON VIOLATION DROP ROW,
  CONSTRAINT estabelecimento_coerente EXPECT (CD_ESTABELECIMENTO IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT com_autorizacao    EXPECT (NU_AUTORIZACAO IS NOT NULL)
) AS
WITH conta_dedup AS (
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
  FROM raw_sia_tb_conta_medica
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY NU_GUIA ORDER BY dt_carga_bronze DESC) = 1
),
procedimento_limpo AS (
  SELECT
    CAST(CD_PROCEDIMENTO AS BIGINT)        AS CD_PROCEDIMENTO,
    NM_PROCEDIMENTO,
    CAST(CD_GRUPO_PROCEDIMENTO AS INT)     AS CD_GRUPO_PROCEDIMENTO,
    NM_GRUPO_PROCEDIMENTO,
    CAST(VL_REFERENCIA AS DECIMAL(18,2))   AS VL_REFERENCIA,
    CAST(FL_ALTA_COMPLEXIDADE AS INT)      AS FL_ALTA_COMPLEXIDADE
  FROM raw_sia_tb_procedimento
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
)
SELECT
  c.NU_GUIA, c.NU_COMPETENCIA, c.DT_ATENDIMENTO, c.DT_APRESENTACAO,
  c.NU_BENEFICIARIO, c.NU_PRESTADOR,
  -- só sobrevive a conta cujo estabelecimento confere com o do prestador
  CASE WHEN c.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO
       THEN c.CD_ESTABELECIMENTO END        AS CD_ESTABELECIMENTO,
  c.CD_PROCEDIMENTO, pr.CD_GRUPO_PROCEDIMENTO, pr.NM_GRUPO_PROCEDIMENTO,
  pr.FL_ALTA_COMPLEXIDADE, c.CD_TIPO_ATENDIMENTO,
  c.QT_ITEM, pr.VL_REFERENCIA,
  c.VL_APRESENTADO, c.VL_GLOSA, c.VL_PAGO,
  c.NU_AUTORIZACAO, c.FL_ATEND_POS_DESCRED
FROM conta_dedup c
INNER JOIN LIVE.sdp_slv_prestador_estabelecimento p ON p.NU_PRESTADOR   = c.NU_PRESTADOR
INNER JOIN LIVE.sdp_slv_beneficiario_plano        b ON b.NU_BENEFICIARIO = c.NU_BENEFICIARIO
INNER JOIN procedimento_limpo                    pr ON pr.CD_PROCEDIMENTO = c.CD_PROCEDIMENTO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver 4 — eventos de credenciamento (histórico + estado atual)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_slv_prestador_evento AS
WITH eventos_historico AS (
  SELECT
    CAST(NU_PRESTADOR AS BIGINT)             AS NU_PRESTADOR,
    CAST(CD_ESTABELECIMENTO AS BIGINT)       AS CD_ESTABELECIMENTO,
    CAST(FL_STATUS_PRESTADOR AS INT)         AS FL_STATUS_PRESTADOR,
    CAST(CD_ESPECIALIDADE AS INT)            AS CD_ESPECIALIDADE,
    CAST(NU_UNIDADE AS INT)                  AS NU_UNIDADE,
    CAST(NU_CAPACIDADE_ATEND_MES AS INT)     AS NU_CAPACIDADE_ATEND_MES,
    CAST(DT_CREDENCIAMENTO AS DATE)          AS DT_CREDENCIAMENTO,
    CAST(DT_DESCREDENCIAMENTO AS DATE)       AS DT_DESCREDENCIAMENTO,
    CAST(CD_MOTIVO_DESCREDENCIAMENTO AS INT) AS CD_MOTIVO_DESCREDENCIAMENTO,
    CAST(DT_AUDIT AS DATE)                   AS DT_AUDIT
  FROM raw_sgr_au_prestador
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
    AND (FL_STATUS_PRESTADOR IS NOT NULL      -- parênteses obrigatórios (módulo 04)
         OR CD_ESPECIALIDADE  IS NOT NULL
         OR NU_UNIDADE        IS NOT NULL
         OR DT_CREDENCIAMENTO IS NOT NULL)
),
eventos_atual AS (
  SELECT
    NU_PRESTADOR, CD_ESTABELECIMENTO, FL_STATUS_PRESTADOR, CD_ESPECIALIDADE,
    NU_UNIDADE, NU_CAPACIDADE_ATEND_MES, DT_CREDENCIAMENTO, DT_DESCREDENCIAMENTO,
    CD_MOTIVO_DESCREDENCIAMENTO,
    CURRENT_DATE() AS DT_AUDIT
  FROM LIVE.sdp_slv_prestador_estabelecimento
)
SELECT * FROM eventos_historico
UNION ALL
SELECT * FROM eventos_atual;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver 5 — vigências (SCD Tipo 2)
-- MAGIC `LAST_VALUE(col, true)` com `ORDER BY ... ASC` = preenchimento **para frente**.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_slv_prestador_vigencia (
  CONSTRAINT vigencia_valida EXPECT (DT_FIM_VIGENCIA > DT_INICIO_VIGENCIA) ON VIOLATION DROP ROW
) AS
WITH vigencias AS (
  SELECT *,
    DT_AUDIT AS DT_FIM_VIGENCIA,
    COALESCE(LAG(DT_AUDIT) OVER (PARTITION BY NU_PRESTADOR ORDER BY DT_AUDIT),
             DT_CREDENCIAMENTO, DT_AUDIT) AS DT_INICIO_VIGENCIA
  FROM LIVE.sdp_slv_prestador_evento
)
SELECT
  NU_PRESTADOR,
  LAST_VALUE(CD_ESTABELECIMENTO, true)      OVER w AS CD_ESTABELECIMENTO,
  LAST_VALUE(FL_STATUS_PRESTADOR, true)     OVER w AS FL_STATUS_PRESTADOR,
  LAST_VALUE(CD_ESPECIALIDADE, true)        OVER w AS CD_ESPECIALIDADE,
  LAST_VALUE(NU_UNIDADE, true)              OVER w AS NU_UNIDADE,
  LAST_VALUE(NU_CAPACIDADE_ATEND_MES, true) OVER w AS NU_CAPACIDADE_ATEND_MES,
  LAST_VALUE(DT_CREDENCIAMENTO, true)       OVER w AS DT_CREDENCIAMENTO,
  CASE WHEN DT_DESCREDENCIAMENTO < DT_FIM_VIGENCIA THEN DT_DESCREDENCIAMENTO END        AS DT_DESCREDENCIAMENTO,
  CASE WHEN DT_DESCREDENCIAMENTO < DT_FIM_VIGENCIA THEN CD_MOTIVO_DESCREDENCIAMENTO END AS CD_MOTIVO_DESCREDENCIAMENTO,
  DT_AUDIT, DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA
FROM vigencias
WINDOW w AS (PARTITION BY NU_PRESTADOR ORDER BY DT_FIM_VIGENCIA ASC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold — custo e utilização por prestador × competência
-- MAGIC Agregação + as-of join + índice de custo normalizado pelos pares.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW sdp_gold_custo_utilizacao (
  CONSTRAINT sk_nao_nula        EXPECT (SK_CUSTO_PRESTADOR_MES IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT custo_nao_negativo EXPECT (VL_CUSTO_TOTAL >= 0)                ON VIOLATION DROP ROW,
  CONSTRAINT indice_calculado   EXPECT (IDX_CUSTO_TABELA IS NOT NULL)
) AS
WITH agregado AS (
  SELECT
    NU_PRESTADOR,
    NU_COMPETENCIA,
    TO_DATE(CAST(NU_COMPETENCIA AS STRING), 'yyyyMM') AS DT_COMPETENCIA,
    COUNT(*)                        AS QT_CONTAS,
    COUNT(DISTINCT NU_BENEFICIARIO) AS QT_BENEFICIARIOS,
    SUM(VL_PAGO)                    AS VL_CUSTO_TOTAL,
    SUM(VL_APRESENTADO)             AS VL_APRESENTADO_TOTAL,
    SUM(VL_GLOSA)                   AS VL_GLOSA_TOTAL,
    SUM(VL_REFERENCIA * QT_ITEM)    AS VL_REFERENCIA_TOTAL,
    SUM(FL_ATEND_POS_DESCRED)       AS QT_CONTAS_POS_DESCREDENCIAMENTO
  FROM LIVE.sdp_slv_conta_medica
  GROUP BY ALL
),
com_vigencia AS (
  SELECT
    a.*,
    v.CD_ESPECIALIDADE, v.FL_STATUS_PRESTADOR, v.NU_CAPACIDADE_ATEND_MES
  FROM agregado a
  INNER JOIN LIVE.sdp_slv_prestador_vigencia v
          ON v.NU_PRESTADOR    = a.NU_PRESTADOR
         AND a.DT_COMPETENCIA >= v.DT_INICIO_VIGENCIA
         AND a.DT_COMPETENCIA  < v.DT_FIM_VIGENCIA
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.NU_PRESTADOR, a.NU_COMPETENCIA ORDER BY v.DT_INICIO_VIGENCIA DESC
  ) = 1
),
com_dimensao AS (
  SELECT c.*, d.NM_PRESTADOR, d.CD_ESTABELECIMENTO, d.NM_ESTABELECIMENTO,
         d.SG_UF, d.NM_REGIAO
  FROM com_vigencia c
  INNER JOIN LIVE.sdp_slv_prestador_estabelecimento d ON d.NU_PRESTADOR = c.NU_PRESTADOR
),
metricas AS (
  SELECT *,
    ROUND(VL_CUSTO_TOTAL / NULLIF(QT_CONTAS, 0), 2)                  AS VL_CUSTO_MEDIO_CONTA,
    ROUND(100 * VL_GLOSA_TOTAL / NULLIF(VL_APRESENTADO_TOTAL, 0), 2) AS PCT_GLOSA,
    ROUND(100.0 * QT_CONTAS / NULLIF(NU_CAPACIDADE_ATEND_MES, 0), 2) AS TX_UTILIZACAO_CAPACIDADE,
    ROUND(VL_CUSTO_TOTAL / NULLIF(VL_REFERENCIA_TOTAL, 0), 4)        AS IDX_CUSTO_TABELA
  FROM com_dimensao
)
SELECT
  XXHASH64(CAST(NU_PRESTADOR AS STRING), CAST(NU_COMPETENCIA AS STRING)) AS SK_CUSTO_PRESTADOR_MES,
  NU_PRESTADOR, NM_PRESTADOR, NU_COMPETENCIA, DT_COMPETENCIA,
  CD_ESPECIALIDADE, FL_STATUS_PRESTADOR,
  CD_ESTABELECIMENTO, NM_ESTABELECIMENTO, SG_UF, NM_REGIAO,
  QT_CONTAS, QT_BENEFICIARIOS, QT_CONTAS_POS_DESCREDENCIAMENTO,
  NU_CAPACIDADE_ATEND_MES, TX_UTILIZACAO_CAPACIDADE,
  VL_CUSTO_TOTAL, VL_APRESENTADO_TOTAL, VL_GLOSA_TOTAL, VL_REFERENCIA_TOTAL,
  VL_CUSTO_MEDIO_CONTA, PCT_GLOSA, IDX_CUSTO_TABELA,
  ROUND(IDX_CUSTO_TABELA / NULLIF(
    AVG(IDX_CUSTO_TABELA) OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA), 0), 4
  )                                                                      AS IDX_CUSTO_VS_PARES,
  CASE WHEN IDX_CUSTO_TABELA / NULLIF(
              AVG(IDX_CUSTO_TABELA) OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA), 0
            ) >= 1.5
            AND QT_CONTAS >= 10
       THEN 1 ELSE 0 END                                                 AS FL_ANOMALIA_CUSTO
FROM metricas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Como executar este notebook como Pipeline
-- MAGIC
-- MAGIC 1. Menu **Lakeflow → Pipelines → Create pipeline** (ou **Workflows → Pipelines**).
-- MAGIC 2. Em **Source code / Paths**, aponte para este notebook (`07_lakeflow_pipeline`).
-- MAGIC 3. Defina o **destino**: catálogo `amil_workshop_trilha_tech` e o **seu** schema
-- MAGIC    (o mesmo onde o setup gerou as tabelas `raw_*` — é dele que o pipeline lê as
-- MAGIC    origens, referenciadas sem qualificação).
-- MAGIC 4. Escolha o modo **Triggered** (roda uma vez) e clique em **Start**.
-- MAGIC 5. Acompanhe o **grafo** (a ordem que o Lakeflow deduziu — você nunca a declarou)
-- MAGIC    e o **painel de expectativas**: quantas linhas passaram e quantas violaram
-- MAGIC    cada `CONSTRAINT`.
-- MAGIC
-- MAGIC ### O que olhar no painel de expectativas
-- MAGIC - `com_autorizacao` (sem `DROP ROW`): mostra o percentual de contas sem
-- MAGIC   autorização **sem** descartá-las — é o equivalente declarativo da métrica
-- MAGIC   `atencao` do módulo 06.
-- MAGIC - `data_coerente`, `valor_nao_negativo`, `pago_ate_apresentado`: as linhas
-- MAGIC   violadas são contadas e **descartadas**. Compare esses números com
-- MAGIC   `qua_conta_invalida` do módulo 03 — devem bater por motivo.
-- MAGIC
-- MAGIC ### Compare as duas implementações
-- MAGIC ```sql
-- MAGIC SELECT
-- MAGIC   (SELECT COUNT(*) FROM <seu_schema>.gold_custo_utilizacao_prestador) AS imperativo,
-- MAGIC   (SELECT COUNT(*) FROM <seu_schema>.sdp_gold_custo_utilizacao)       AS declarativo;
-- MAGIC ```
-- MAGIC
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Por que o pipeline não precisou que ninguém declarasse a ordem das tabelas?
-- MAGIC 2. O que foi **perdido** ao trocar a quarentena por `ON VIOLATION DROP ROW`?
