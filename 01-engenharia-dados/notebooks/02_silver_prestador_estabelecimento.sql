-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 02 — Silver: dimensões conformadas da rede e da carteira
-- MAGIC
-- MAGIC A área de negócio quer custo **por prestador, por especialidade, por região**.
-- MAGIC Nada disso existe em uma tabela só: o prestador está no SGR, a região está no
-- MAGIC cadastro do estabelecimento, o plano está no SGB. A silver é onde essas peças
-- MAGIC viram **dimensões conformadas** — uma linha por entidade, tipada, sem duplicata
-- MAGIC e com as chaves fechando.
-- MAGIC
-- MAGIC **As 4 camadas de qualidade aplicadas aqui:**
-- MAGIC
-- MAGIC | # | Camada | Técnica |
-- MAGIC |---|---|---|
-- MAGIC | 1 | **Tipagem** | `CAST` de `DECIMAL(38,10)`/`TIMESTAMP` para `BIGINT`/`INT`/`DATE` |
-- MAGIC | 2 | **Filtro de exclusão** | descartar `FL_EXCLUIDO = 1` |
-- MAGIC | 3 | **Deduplicação** | `QUALIFY ROW_NUMBER()` — a versão mais recente por chave |
-- MAGIC | 4 | **Integridade referencial** | `INNER JOIN` + **quarentena** dos órfãos |
-- MAGIC
-- MAGIC A camada 4 é a que separa um pipeline profissional de um `LEFT JOIN`
-- MAGIC descuidado: registro que não fecha chave **não some** — ele vai para uma
-- MAGIC tabela `qua_*` com o motivo, e aparece no painel de qualidade (módulo 06).

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — por que precisamos de dedup no estabelecimento
-- MAGIC A origem manda o cadastro completo a cada carga. Rode e veja que alguns
-- MAGIC estabelecimentos aparecem **duas vezes**, com `dt_carga_bronze` diferente.

-- COMMAND ----------

SELECT CD_ESTABELECIMENTO, NM_ESTABELECIMENTO, NU_LEITOS, dt_carga_bronze
FROM IDENTIFIER(meu_schema || '.brz_estabelecimento')
QUALIFY COUNT(*) OVER (PARTITION BY CD_ESTABELECIMENTO) > 1
ORDER BY CD_ESTABELECIMENTO, dt_carga_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — `slv_prestador_estabelecimento`
-- MAGIC
-- MAGIC Leia com atenção: este é o **modelo** que você vai repetir no exercício.
-- MAGIC Cada comentário `-- QUALIDADE:` marca uma das 4 camadas.
-- MAGIC
-- MAGIC Note também o enriquecimento de domínio: `CD_ESPECIALIDADE` é um código que
-- MAGIC ninguém do negócio entende, então derivamos `NM_ESPECIALIDADE` aqui, uma única
-- MAGIC vez, em vez de repetir o `CASE` em cada relatório.

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
  WHERE CAST(FL_EXCLUIDO AS INT) = 0                          -- QUALIDADE 2: remove excluídos
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY NU_PRESTADOR ORDER BY dt_carga_bronze DESC
  ) = 1                                                       -- QUALIDADE 3: 1 linha por prestador
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
    CAST(NU_CNPJ AS DECIMAL(38,0))          AS NU_CNPJ                -- a coluna preenchida
  FROM IDENTIFIER(meu_schema || '.brz_estabelecimento')
  WHERE CAST(FL_EXCLUIDO AS INT) = 0
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CD_ESTABELECIMENTO ORDER BY dt_carga_bronze DESC
  ) = 1
)
SELECT
  p.NU_PRESTADOR, p.CD_CNES, p.NM_PRESTADOR,
  p.FL_STATUS_PRESTADOR, p.CD_ESPECIALIDADE,
  CASE p.CD_ESPECIALIDADE                                     -- padronização de domínio
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
INNER JOIN estabelecimento_limpo e                            -- QUALIDADE 4: só quem fecha chave
        ON e.CD_ESTABELECIMENTO = p.CD_ESTABELECIMENTO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 (com o Assistant) — quarentena dos prestadores órfãos
-- MAGIC
-- MAGIC O `INNER JOIN` acima **silenciou** os prestadores cujo `CD_ESTABELECIMENTO`
-- MAGIC não existe no cadastro. Precisamos saber quem são.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela IDENTIFIER(meu_schema || '.qua_prestador_orfao') com os
-- MAGIC > prestadores de brz_prestador que não têm estabelecimento correspondente em
-- MAGIC > brz_estabelecimento. Use LEFT JOIN por CD_ESTABELECIMENTO filtrando onde o
-- MAGIC > estabelecimento é nulo, considere apenas FL_EXCLUIDO = 0, e traga as colunas
-- MAGIC > NU_PRESTADOR, CD_ESTABELECIMENTO, um texto fixo como motivo_quarentena e
-- MAGIC > CURRENT_TIMESTAMP() como dt_quarentena."_

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 (com o Assistant) — a dimensão de carteira
-- MAGIC
-- MAGIC Agora aplique **o mesmo padrão** do outro lado do modelo: beneficiário + plano.
-- MAGIC A técnica é idêntica; só mudam as tabelas e as chaves. É assim que se ganha
-- MAGIC velocidade em engenharia de dados — reconhecer o padrão, não decorar o SQL.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela IDENTIFIER(meu_schema || '.slv_beneficiario_plano') juntando
-- MAGIC > brz_beneficiario com brz_plano por CD_PLANO, seguindo o mesmo padrão da
-- MAGIC > tabela slv_prestador_estabelecimento: CAST dos decimais para BIGINT/INT,
-- MAGIC > datas para DATE, filtrar FL_EXCLUIDO = 0 nas duas tabelas, deduplicar com
-- MAGIC > QUALIFY ROW_NUMBER por NU_BENEFICIARIO e por CD_PLANO ordenando por
-- MAGIC > dt_carga_bronze DESC, e usar INNER JOIN. Traga NU_BENEFICIARIO, NU_CARTEIRA,
-- MAGIC > SG_UF, NM_REGIAO, NU_FAIXA_ETARIA, CD_SEXO, DT_ADESAO, DT_CANCELAMENTO,
-- MAGIC > FL_ATIVO, CD_PLANO, NM_PLANO, CD_SEGMENTACAO, NM_ACOMODACAO,
-- MAGIC > FL_COPARTICIPACAO e VL_MENSALIDADE_BASE."_
-- MAGIC
-- MAGIC > 💡 Note que aqui **não há órfãos** — todo plano referenciado existe. Nem toda
-- MAGIC > origem tem defeito, e verificar isso faz parte do trabalho.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência
-- MAGIC `validos + em_quarentena` deve ser igual ao total de prestadores não excluídos.

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento')) AS prestadores_validos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.qua_prestador_orfao'))           AS prestadores_em_quarentena,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.slv_beneficiario_plano'))        AS beneficiarios_validos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.brz_prestador')
    WHERE CAST(FL_EXCLUIDO AS INT) = 0)                                             AS prestadores_nao_excluidos;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Se você trocasse `QUALIFY ROW_NUMBER()` por `DISTINCT`, o que aconteceria
-- MAGIC    com os estabelecimentos que têm duas versões?
-- MAGIC 2. Por que o `CASE` da especialidade mora na silver e não no relatório?
-- MAGIC 3. `NU_INSCRICAO_MUNICIPAL` existe na origem e está toda nula. Como você
-- MAGIC    descobriu que a coluna certa era `NU_CNPJ`?
