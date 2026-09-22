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
-- MAGIC # Módulo 06 — Qualidade de dados: painel e porta de qualidade
-- MAGIC
-- MAGIC Nos módulos 02–05 as regras de qualidade ficaram **dentro** das transformações.
-- MAGIC Isso é correto, mas insuficiente: ninguém consegue auditar dezenas de `WHERE`
-- MAGIC espalhados em cinco notebooks.
-- MAGIC
-- MAGIC Aqui consolidamos tudo em uma tabela `dq_metricas` — uma linha por métrica —
-- MAGIC que funciona como **porta de qualidade** (*quality gate*) do pipeline: se
-- MAGIC houver métrica com severidade `erro`, os dados **não** vão para o negócio.
-- MAGIC
-- MAGIC | Severidade | Significado | Ação |
-- MAGIC |---|---|---|
-- MAGIC | `ok` | dentro do esperado | segue |
-- MAGIC | `informativo` | só contexto/volumetria | segue |
-- MAGIC | `atencao` | achado de operação, não de dado | segue, mas notifica o negócio |
-- MAGIC | `erro` | o número está errado | **para o pipeline** |
-- MAGIC
-- MAGIC A distinção entre `atencao` e `erro` é a parte que mais se erra na prática:
-- MAGIC atendimento após descredenciamento é um problema **real da operadora**, mas o
-- MAGIC dado está certo — parar o pipeline por isso só faria o negócio perder o alerta.

-- COMMAND ----------

-- 👇 TROQUE `seu_usuario` pelo nome do schema que o setup criou para você.
--    É o seu e-mail antes do @, com o ponto trocado por underscore.
--    Ex.: maria.silva@amil.com.br  ->  maria_silva
USE CATALOG amil_workshop_trilha_tech;
USE SCHEMA seu_usuario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — o padrão do painel
-- MAGIC Três métricas de exemplo. Cada bloco do `UNION ALL` devolve
-- MAGIC `metrica`, `valor` (como STRING, para caber qualquer tipo) e `severidade`.

-- COMMAND ----------

CREATE OR REPLACE TABLE dq_metricas AS
SELECT 'silver_prestadores_validos' AS metrica,
       CAST(COUNT(*) AS STRING)     AS valor,
       'informativo'                AS severidade
FROM slv_prestador_estabelecimento

UNION ALL
SELECT 'quarentena_prestador_orfao',
       CAST(COUNT(*) AS STRING),
       CASE WHEN COUNT(*) > 0 THEN 'atencao' ELSE 'ok' END
FROM qua_prestador_orfao

UNION ALL
SELECT 'gold_sk_duplicadas',
       CAST(COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) AS STRING),
       CASE WHEN COUNT(*) - COUNT(DISTINCT SK_CUSTO_PRESTADOR_MES) > 0 THEN 'erro' ELSE 'ok' END
FROM gold_custo_utilizacao_prestador;

-- COMMAND ----------

SELECT * FROM dq_metricas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício-chave (com o Assistant) — completar o painel
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Reescreva a tabela dq_metricas mantendo as três
-- MAGIC > métricas atuais e acrescentando, no mesmo padrão (metrica, valor como STRING,
-- MAGIC > severidade), com UNION ALL:_
-- MAGIC >
-- MAGIC > _1. 'silver_contas_validas': contagem de slv_conta_medica, informativo;_
-- MAGIC > _2. 'quarentena_contas': contagem de qua_conta_invalida, atencao se maior que zero;_
-- MAGIC > _3. 'quarentena_valor_retido': soma de VL_PAGO em qua_conta_invalida, informativo;_
-- MAGIC > _4. 'contas_sem_autorizacao': contagem de slv_conta_medica com NU_AUTORIZACAO
-- MAGIC >    nulo, atencao se maior que zero;_
-- MAGIC > _5. 'contas_pos_descredenciamento': soma de FL_ATEND_POS_DESCRED em
-- MAGIC >    slv_conta_medica, atencao se maior que zero;_
-- MAGIC > _6. 'gold_custo_negativo': contagem de linhas da gold com VL_CUSTO_TOTAL menor
-- MAGIC >    que zero, erro se maior que zero;_
-- MAGIC > _7. 'gold_idx_custo_nulo': contagem de linhas da gold com IDX_CUSTO_TABELA
-- MAGIC >    nulo, erro se maior que zero;_
-- MAGIC > _8. 'gold_prestadores_anomalos': soma de FL_ANOMALIA_CUSTO na gold, atencao se
-- MAGIC >    maior que zero;_
-- MAGIC > _9. 'reconciliacao_custo_silver_gold': diferença absoluta arredondada em 2
-- MAGIC >    casas entre a soma de VL_PAGO em slv_conta_medica e a soma de
-- MAGIC >    VL_CUSTO_TOTAL na gold, com severidade erro se a diferença for maior que
-- MAGIC >    0.01 e ok caso contrário."_
-- MAGIC
-- MAGIC 💡 A métrica 9 é a mais importante de todas: ela prova que a agregação da gold
-- MAGIC não perdeu nem inventou dinheiro em relação à silver. É o tipo de checagem que
-- MAGIC separa um pipeline confiável de um pipeline que "parece certo".

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Painel de qualidade

-- COMMAND ----------

SELECT * FROM dq_metricas
ORDER BY CASE severidade WHEN 'erro' THEN 1 WHEN 'atencao' THEN 2
                         WHEN 'informativo' THEN 3 ELSE 4 END, metrica;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Porta de qualidade
-- MAGIC Se esta query retornar **qualquer linha**, o pipeline não deveria publicar.
-- MAGIC Em um Lakeflow Job, esta célula seria a última tarefa antes da publicação.

-- COMMAND ----------

SELECT * FROM dq_metricas WHERE severidade = 'erro';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. Por que a reconciliação silver↔gold pode passar mesmo com a gold errada?
-- MAGIC    (dica: pense em duplicação que se cancela)
-- MAGIC 2. Se a quarentena de contas dobrasse de um dia para o outro, isso é `atencao`
-- MAGIC    ou `erro`? O que define o limiar?
