-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 04 — Silver: vigências do credenciamento (SCD2) (GABARITO)
-- MAGIC
-- MAGIC Produz `slv_prestador_evento` (linha do tempo unificada) e
-- MAGIC `slv_prestador_vigencia` (intervalos SCD Tipo 2), que é a dimensão histórica
-- MAGIC usada no as-of join do módulo 05.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 — histórico (auditoria) + estado atual
-- MAGIC
-- MAGIC Atenção aos **parênteses** no bloco de `OR`: sem eles, `FL_EXCLUIDO = 0`
-- MAGIC deixa de valer e os eventos estornados voltam para a linha do tempo.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.slv_prestador_evento') AS
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
  FROM IDENTIFIER(meu_schema || '.brz_prestador_auditoria')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
    -- QUALIDADE: os parênteses corrigem a precedência entre AND e OR.
    -- Sem eles, o filtro FL_EXCLUIDO seria anulado pelos OR seguintes.
    AND (
      FL_STATUS_PRESTADOR IS NOT NULL
      OR CD_ESPECIALIDADE  IS NOT NULL
      OR NU_UNIDADE        IS NOT NULL
      OR DT_CREDENCIAMENTO IS NOT NULL
    )
),
eventos_atual AS (
  SELECT
    NU_PRESTADOR, CD_ESTABELECIMENTO, FL_STATUS_PRESTADOR, CD_ESPECIALIDADE,
    NU_UNIDADE, NU_CAPACIDADE_ATEND_MES, DT_CREDENCIAMENTO, DT_DESCREDENCIAMENTO,
    CD_MOTIVO_DESCREDENCIAMENTO,
    CURRENT_DATE() AS DT_AUDIT
  FROM IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento')
)
SELECT * FROM eventos_historico
UNION ALL
SELECT * FROM eventos_atual;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 — vigências (SCD2) com preenchimento para frente
-- MAGIC
-- MAGIC `LAST_VALUE(col, true)` sobre janela `ORDER BY DT_FIM_VIGENCIA ASC ROWS BETWEEN
-- MAGIC UNBOUNDED PRECEDING AND CURRENT ROW` = "o último valor não-nulo **até aqui**".
-- MAGIC É o *forward fill* correto, sem vazamento de informação do futuro.

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.slv_prestador_vigencia') AS
WITH vigencias AS (
  SELECT
    *,
    DT_AUDIT AS DT_FIM_VIGENCIA,
    COALESCE(
      LAG(DT_AUDIT) OVER (PARTITION BY NU_PRESTADOR ORDER BY DT_AUDIT),
      DT_CREDENCIAMENTO,
      DT_AUDIT
    ) AS DT_INICIO_VIGENCIA
  FROM IDENTIFIER(meu_schema || '.slv_prestador_evento')
),
preenchido AS (
  SELECT
    NU_PRESTADOR,
    LAST_VALUE(CD_ESTABELECIMENTO, true)      OVER w AS CD_ESTABELECIMENTO,
    LAST_VALUE(FL_STATUS_PRESTADOR, true)     OVER w AS FL_STATUS_PRESTADOR,
    LAST_VALUE(CD_ESPECIALIDADE, true)        OVER w AS CD_ESPECIALIDADE,
    LAST_VALUE(NU_UNIDADE, true)              OVER w AS NU_UNIDADE,
    LAST_VALUE(NU_CAPACIDADE_ATEND_MES, true) OVER w AS NU_CAPACIDADE_ATEND_MES,
    LAST_VALUE(DT_CREDENCIAMENTO, true)       OVER w AS DT_CREDENCIAMENTO,
    -- descredenciamento só "vale" a partir do momento em que aconteceu
    CASE WHEN DT_DESCREDENCIAMENTO < DT_FIM_VIGENCIA THEN DT_DESCREDENCIAMENTO END          AS DT_DESCREDENCIAMENTO,
    CASE WHEN DT_DESCREDENCIAMENTO < DT_FIM_VIGENCIA THEN CD_MOTIVO_DESCREDENCIAMENTO END   AS CD_MOTIVO_DESCREDENCIAMENTO,
    DT_AUDIT,
    DT_INICIO_VIGENCIA,
    DT_FIM_VIGENCIA
  FROM vigencias
  WINDOW w AS (
    PARTITION BY NU_PRESTADOR
    ORDER BY DT_FIM_VIGENCIA ASC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
)
SELECT *
FROM preenchido
WHERE DT_FIM_VIGENCIA > DT_INICIO_VIGENCIA;   -- QUALIDADE: vigência de duração positiva

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferências

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_prestador_evento'))   AS eventos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_prestador_vigencia')) AS vigencias,
  (SELECT COUNT(DISTINCT NU_PRESTADOR) FROM IDENTIFIER(meu_schema || '.slv_prestador_vigencia')) AS prestadores;

-- COMMAND ----------

-- Sobreposição de vigências: deve retornar zero linhas
SELECT NU_PRESTADOR, DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA, fim_anterior
FROM (
  SELECT NU_PRESTADOR, DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA,
         LAG(DT_FIM_VIGENCIA) OVER (PARTITION BY NU_PRESTADOR ORDER BY DT_INICIO_VIGENCIA) AS fim_anterior
  FROM IDENTIFIER(meu_schema || '.slv_prestador_vigencia')
)
WHERE fim_anterior > DT_INICIO_VIGENCIA;

-- COMMAND ----------

-- O histórico varia? (se tudo for 1, o preenchimento foi feito para trás)
SELECT qt_especialidades_distintas, COUNT(*) AS prestadores
FROM (
  SELECT NU_PRESTADOR, COUNT(DISTINCT CD_ESPECIALIDADE) AS qt_especialidades_distintas
  FROM IDENTIFIER(meu_schema || '.slv_prestador_vigencia')
  GROUP BY NU_PRESTADOR
)
GROUP BY ALL
ORDER BY qt_especialidades_distintas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. Ele terá **duas** linhas de evento (o evento de credenciamento + o estado
-- MAGIC    atual) e, portanto, uma ou duas vigências. Com `LAG` nulo no primeiro
-- MAGIC    evento, o `COALESCE` usa `DT_CREDENCIAMENTO` como início — por isso a ordem
-- MAGIC    dos argumentos do `COALESCE` importa.
-- MAGIC 2. Porque vigência de duração zero ou negativa não representa período algum e
-- MAGIC    **duplicaria** a conta médica no as-of join (dois intervalos contendo a mesma
-- MAGIC    data). Acontece quando dois eventos caem no mesmo dia.
-- MAGIC 3. A conta "pousa" pelo par `NU_PRESTADOR` + data: a competência da conta tem
-- MAGIC    de cair no intervalo `[DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA)`. É o as-of join
-- MAGIC    do módulo 05.
