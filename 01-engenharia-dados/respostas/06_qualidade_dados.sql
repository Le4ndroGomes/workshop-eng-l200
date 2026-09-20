-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 06 — Qualidade de dados: painel e porta de qualidade (GABARITO)
-- MAGIC
-- MAGIC Painel `dq_metricas` consolidado: volumetria, quarentenas, achados de operação,
-- MAGIC integridade da gold e reconciliação de custo silver↔gold.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício — painel completo

-- COMMAND ----------

CREATE OR REPLACE TABLE IDENTIFIER(meu_schema || '.dq_metricas') AS

-- ---------- volumetria ----------
SELECT 'silver_prestadores_validos' AS metrica,
       CAST(COUNT(*) AS STRING)     AS valor,
       'informativo'                AS severidade
FROM IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento')

UNION ALL
SELECT 'silver_contas_validas', CAST(COUNT(*) AS STRING), 'informativo'
FROM IDENTIFIER(meu_schema || '.slv_conta_medica')

UNION ALL
SELECT 'gold_linhas', CAST(COUNT(*) AS STRING), 'informativo'
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')

-- ---------- quarentenas ----------
UNION ALL
SELECT 'quarentena_prestador_orfao',
       CAST(COUNT(*) AS STRING),
       CASE WHEN COUNT(*) > 0 THEN 'atencao' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.qua_prestador_orfao')

UNION ALL
SELECT 'quarentena_contas',
       CAST(COUNT(*) AS STRING),
       CASE WHEN COUNT(*) > 0 THEN 'atencao' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.qua_conta_invalida')

UNION ALL
SELECT 'quarentena_valor_retido',
       CAST(ROUND(COALESCE(SUM(VL_PAGO), 0), 2) AS STRING),
       'informativo'
FROM IDENTIFIER(meu_schema || '.qua_conta_invalida')

-- ---------- achados de operação (dado certo, problema real) ----------
UNION ALL
SELECT 'contas_sem_autorizacao',
       CAST(COUNT_IF(NU_AUTORIZACAO IS NULL) AS STRING),
       CASE WHEN COUNT_IF(NU_AUTORIZACAO IS NULL) > 0 THEN 'atencao' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.slv_conta_medica')

UNION ALL
SELECT 'contas_pos_descredenciamento',
       CAST(COALESCE(SUM(FL_ATEND_POS_DESCRED), 0) AS STRING),
       CASE WHEN COALESCE(SUM(FL_ATEND_POS_DESCRED), 0) > 0 THEN 'atencao' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.slv_conta_medica')

UNION ALL
SELECT 'gold_prestadores_anomalos',
       CAST(COALESCE(SUM(FL_ANOMALIA_CUSTO), 0) AS STRING),
       CASE WHEN COALESCE(SUM(FL_ANOMALIA_CUSTO), 0) > 0 THEN 'atencao' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')

-- ---------- integridade estrutural da gold (erro = para o pipeline) ----------
UNION ALL
SELECT 'gold_sk_duplicadas',
       CAST(COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) AS STRING),
       CASE WHEN COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) > 0 THEN 'erro' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')

UNION ALL
SELECT 'gold_custo_negativo',
       CAST(COUNT_IF(VL_CUSTO_TOTAL < 0) AS STRING),
       CASE WHEN COUNT_IF(VL_CUSTO_TOTAL < 0) > 0 THEN 'erro' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')

UNION ALL
SELECT 'gold_idx_custo_nulo',
       CAST(COUNT_IF(IDX_CUSTO_TABELA IS NULL) AS STRING),
       CASE WHEN COUNT_IF(IDX_CUSTO_TABELA IS NULL) > 0 THEN 'erro' ELSE 'ok' END
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')

-- ---------- reconciliação: a gold não pode perder nem inventar dinheiro ----------
UNION ALL
SELECT 'reconciliacao_custo_silver_gold',
       CAST(ROUND(ABS(s.total - g.total), 2) AS STRING),
       CASE WHEN ROUND(ABS(s.total - g.total), 2) > 0.01 THEN 'erro' ELSE 'ok' END
FROM      (SELECT SUM(VL_PAGO)        AS total FROM IDENTIFIER(meu_schema || '.slv_conta_medica')) s
CROSS JOIN (SELECT SUM(VL_CUSTO_TOTAL) AS total FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')) g;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Painel

-- COMMAND ----------

SELECT * FROM IDENTIFIER(meu_schema || '.dq_metricas')
ORDER BY CASE severidade WHEN 'erro' THEN 1 WHEN 'atencao' THEN 2
                         WHEN 'informativo' THEN 3 ELSE 4 END, metrica;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Porta de qualidade — deve retornar zero linhas

-- COMMAND ----------

SELECT * FROM IDENTIFIER(meu_schema || '.dq_metricas') WHERE severidade = 'erro';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Detalhe de apoio — quarentena por motivo
-- MAGIC Útil na discussão: mostra onde está concentrado o dinheiro retido.

-- COMMAND ----------

SELECT motivo_quarentena,
       COUNT(*)                    AS contas,
       ROUND(SUM(VL_PAGO), 2)      AS valor_retido
FROM IDENTIFIER(meu_schema || '.qua_conta_invalida')
GROUP BY ALL
ORDER BY valor_retido DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Respostas do checkpoint
-- MAGIC
-- MAGIC 1. Porque a reconciliação só compara **totais**. Se a gold atribuísse contas ao
-- MAGIC    prestador errado, ou se duas linhas dividissem o custo de forma incorreta
-- MAGIC    mantendo a soma, o total continuaria igual. Totais provam ausência de
-- MAGIC    perda/duplicação **global**, não correção da distribuição — por isso a SK
-- MAGIC    única e a conferência de linhas do módulo 05 continuam necessárias.
-- MAGIC    Reconciliação e teste de grão são checagens complementares, não redundantes.
-- MAGIC 2. `atencao`. O dado em si pode estar perfeitamente correto: quem dobrou foi o
-- MAGIC    problema na origem, não o pipeline. O limiar vem de um **baseline**: guarda-se
-- MAGIC    o histórico do próprio painel e compara-se com a média das execuções
-- MAGIC    anteriores. Sem histórico, qualquer limiar absoluto é chute — motivo pelo
-- MAGIC    qual o painel é uma **tabela**, e não uma query descartável.
