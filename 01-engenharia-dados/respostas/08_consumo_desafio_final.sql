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
-- MAGIC # Módulo 08 — Consumo, orquestração e desafio final (GABARITO)
-- MAGIC
-- MAGIC > ⚠️ **Para o instrutor.** Não distribua antes da turma tentar. O valor do
-- MAGIC > desafio está na descoberta da **segunda** causa, que o indicador de anomalia
-- MAGIC > não pega por construção.
-- MAGIC
-- MAGIC ## O que está plantado nos dados
-- MAGIC | Causa | Natureza | Onde aparece | Pega no `FL_ANOMALIA_CUSTO`? |
-- MAGIC |---|---|---|---|
-- MAGIC | **A — sobrepreço concentrado**: 10 prestadores passam a cobrar ~4× a tabela a partir de 202510 | concentrada, poucos CNPJs | `IDX_CUSTO_VS_PARES` alto | **Sim** |
-- MAGIC | **B — inflação regional**: todos os prestadores de dois estabelecimentos do **Nordeste** (Salvador/BA e Recife/PE) sobem ~60% a partir de 202510 | difusa, região inteira | só por agregação regional | **Não** |
-- MAGIC
-- MAGIC A causa B é invisível ao indicador porque ele normaliza pelos **pares da
-- MAGIC especialidade**: quando o grupo todo sobe, o índice relativo não se move.
-- MAGIC Esta é a lição central do desafio: **nenhum indicador único cobre todo tipo de
-- MAGIC anomalia — a dimensão de análise faz parte da detecção.**
-- MAGIC
-- MAGIC Os prestadores da causa A são fixos e reprodutíveis: `NU_PRESTADOR` 70007,
-- MAGIC 70023, 70041, 70088, 70152, 70246, 70333, 70417, 70501 e 70588.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 0 — a porta de qualidade
-- MAGIC Antes de responder ao CFO: nenhuma métrica com severidade `erro`.

-- COMMAND ----------

SELECT * FROM IDENTIFIER(meu_schema || '.dq_metricas') WHERE severidade = 'erro';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 1 — volume ou preço?
-- MAGIC `VL_REFERENCIA_TOTAL` só cresce com **volume e mix**; `VL_CUSTO_TOTAL` cresce
-- MAGIC também com **preço**. A razão entre os dois isola o efeito preço.

-- COMMAND ----------

SELECT
  NU_COMPETENCIA,
  SUM(QT_CONTAS)                                              AS contas,
  ROUND(SUM(VL_REFERENCIA_TOTAL) / 1000000, 2)                AS referencia_milhoes,
  ROUND(SUM(VL_CUSTO_TOTAL) / 1000000, 2)                     AS custo_milhoes,
  ROUND(SUM(VL_CUSTO_TOTAL) / SUM(VL_REFERENCIA_TOTAL), 4)    AS idx_custo_tabela
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Leitura esperada:** o volume de contas fica estável e o
-- MAGIC `referencia_milhoes` também; o `custo_milhoes` e o `idx_custo_tabela` sobem a
-- MAGIC partir de **202510**. Conclusão: o aumento é de **preço**, não de utilização.
-- MAGIC Isso já elimina metade das hipóteses que a diretoria costuma levantar.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 2 — onde? (região × competência)

-- COMMAND ----------

SELECT
  NM_REGIAO,
  ROUND(SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_CUSTO_TOTAL END) /
        SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_REFERENCIA_TOTAL END), 4) AS idx_base,
  ROUND(SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_CUSTO_TOTAL END) /
        SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_REFERENCIA_TOTAL END), 4) AS idx_recente,
  ROUND(100 * (
    SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_CUSTO_TOTAL END) /
    SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_REFERENCIA_TOTAL END)
    /
    NULLIF(SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_CUSTO_TOTAL END) /
           SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_REFERENCIA_TOTAL END), 0)
    - 1), 1)                                                                     AS variacao_pct
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY variacao_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Leitura esperada:** o **Nordeste** aparece com a maior variação, bem acima das
-- MAGIC demais regiões. Repare que nenhum prestador do Nordeste precisa estar sinalizado
-- MAGIC como anômalo para isso aparecer — a causa é visível **só** neste grão.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 3 — causa A: sobrepreço concentrado, quantificado em R$
-- MAGIC Custo excedente = `VL_REFERENCIA_TOTAL × (IDX_observado − IDX_base_do_proprio_prestador)`.

-- COMMAND ----------

WITH base AS (   -- o índice de cada prestador antes do evento
  SELECT NU_PRESTADOR,
         SUM(VL_CUSTO_TOTAL) / NULLIF(SUM(VL_REFERENCIA_TOTAL), 0) AS idx_base
  FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
  WHERE NU_COMPETENCIA <= 202509
  GROUP BY NU_PRESTADOR
),
recente AS (
  SELECT g.NU_PRESTADOR, g.NM_PRESTADOR, g.NM_ESPECIALIDADE, g.NM_REGIAO,
         SUM(g.VL_CUSTO_TOTAL)      AS custo,
         SUM(g.VL_REFERENCIA_TOTAL) AS referencia,
         MAX(g.FL_ANOMALIA_CUSTO)   AS sinalizado
  FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador') g
  WHERE g.NU_COMPETENCIA >= 202510
  GROUP BY ALL
)
SELECT
  r.NU_PRESTADOR, r.NM_PRESTADOR, r.NM_ESPECIALIDADE, r.NM_REGIAO, r.sinalizado,
  ROUND(b.idx_base, 3)                                            AS idx_base,
  ROUND(r.custo / NULLIF(r.referencia, 0), 3)                     AS idx_recente,
  ROUND(r.custo - r.referencia * b.idx_base, 2)                   AS custo_excedente
FROM recente r
INNER JOIN base b ON b.NU_PRESTADOR = r.NU_PRESTADOR
WHERE r.sinalizado = 1
ORDER BY custo_excedente DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 4 — causa B: inflação regional, quantificada em R$
-- MAGIC Mesma conta, no grão da região, **excluindo** os prestadores já explicados pela
-- MAGIC causa A — senão o mesmo dinheiro seria contado duas vezes.

-- COMMAND ----------

WITH sinalizados AS (
  SELECT DISTINCT NU_PRESTADOR
  FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
  WHERE FL_ANOMALIA_CUSTO = 1
),
sem_causa_a AS (
  SELECT g.*
  FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador') g
  LEFT ANTI JOIN sinalizados s ON s.NU_PRESTADOR = g.NU_PRESTADOR
),
por_regiao AS (
  SELECT
    NM_REGIAO,
    SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_CUSTO_TOTAL END)      AS custo_base,
    SUM(CASE WHEN NU_COMPETENCIA <= 202509 THEN VL_REFERENCIA_TOTAL END) AS ref_base,
    SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_CUSTO_TOTAL END)      AS custo_rec,
    SUM(CASE WHEN NU_COMPETENCIA >= 202510 THEN VL_REFERENCIA_TOTAL END) AS ref_rec
  FROM sem_causa_a
  GROUP BY ALL
)
SELECT
  NM_REGIAO,
  ROUND(custo_base / NULLIF(ref_base, 0), 4)                        AS idx_base,
  ROUND(custo_rec  / NULLIF(ref_rec, 0), 4)                         AS idx_recente,
  ROUND(custo_rec - ref_rec * (custo_base / NULLIF(ref_base, 0)), 2) AS custo_excedente
FROM por_regiao
ORDER BY custo_excedente DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 5 — onde exatamente, dentro do Nordeste

-- COMMAND ----------

SELECT
  g.NM_ESTABELECIMENTO, d.NM_MUNICIPIO, g.SG_UF,
  g.NU_COMPETENCIA,
  COUNT(DISTINCT g.NU_PRESTADOR)                                          AS prestadores,
  ROUND(SUM(g.VL_CUSTO_TOTAL) / NULLIF(SUM(g.VL_REFERENCIA_TOTAL), 0), 4) AS idx_custo_tabela
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador') g
INNER JOIN IDENTIFIER(meu_schema || '.slv_prestador_estabelecimento') d
        ON d.NU_PRESTADOR = g.NU_PRESTADOR
WHERE g.NM_REGIAO = 'Nordeste'
GROUP BY ALL
ORDER BY g.NM_ESTABELECIMENTO, g.NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Passo 6 — o achado de brinde (operacional)
-- MAGIC Vale mostrar: contas de prestadores já descredenciados continuaram entrando.
-- MAGIC Não explica o aumento de custo, mas é dinheiro pago fora da rede vigente — e
-- MAGIC só foi possível detectar porque a dimensão é **historicizada** (módulo 04).

-- COMMAND ----------

SELECT
  NU_COMPETENCIA,
  SUM(QT_CONTAS_POS_DESCREDENCIAMENTO) AS contas_pos_descredenciamento
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Resposta modelo à Diretoria
-- MAGIC
-- MAGIC > O aumento do trimestre é de **preço, não de utilização**: o volume de contas e
-- MAGIC > o valor de tabela dos procedimentos realizados ficaram estáveis, enquanto o
-- MAGIC > índice de custo sobre tabela subiu a partir da competência 202510.
-- MAGIC >
-- MAGIC > Há **duas causas independentes**:
-- MAGIC >
-- MAGIC > **1. Sobrepreço concentrado em 10 prestadores** (lista em anexo), que passaram
-- MAGIC > a cobrar múltiplos do valor de referência de seus procedimentos, fora do padrão
-- MAGIC > de seus pares de especialidade. Custo excedente estimado: **R$ X**.
-- MAGIC > *Ação:* auditoria de conta médica e renegociação contratual imediata —
-- MAGIC > Gestão de Rede, com apoio jurídico.
-- MAGIC >
-- MAGIC > **2. Inflação difusa na região Nordeste**, concentrada em dois
-- MAGIC > estabelecimentos (Salvador/BA e Recife/PE), em que **todos** os prestadores
-- MAGIC > subiram juntos. Custo excedente estimado: **R$ Y**.
-- MAGIC > *Ação:* revisão da tabela de repasse regional e verificação de reajuste
-- MAGIC > aplicado sem aprovação — Gestão de Rede regional e Controladoria.
-- MAGIC >
-- MAGIC > Como achado adicional, identificamos contas pagas a prestadores já
-- MAGIC > descredenciados na data do atendimento. *Ação:* bloqueio na autorização —
-- MAGIC > Operações.
-- MAGIC >
-- MAGIC > Todos os números foram apurados sobre a base que passou pela porta de
-- MAGIC > qualidade; R$ Z estão retidos em quarentena por inconsistência de cadastro e
-- MAGIC > não entram nestes totais.
-- MAGIC
-- MAGIC ### Respostas do checkpoint
-- MAGIC 1. Se a turma parou na causa A, o `FL_ANOMALIA_CUSTO` funcionou como âncora
-- MAGIC    mental — é o ponto para discutir que um indicador define **o que se pode
-- MAGIC    enxergar**. Pergunte: "que aumento este indicador jamais pegaria?"
-- MAGIC 2. Com custo médio por conta, o ruído do mix de procedimentos produz uma lista
-- MAGIC    de "prestadores caros" que são, na verdade, prestadores que fizeram
-- MAGIC    procedimentos de alta complexidade. Bom momento para revisitar o módulo 05.
-- MAGIC 3. Quem não conferiu `dq_metricas` respondeu ao CFO sem saber se o número era
-- MAGIC    confiável. Em produção, é exatamente assim que se perde a confiança do
-- MAGIC    negócio no dado — de uma vez e por muito tempo.
