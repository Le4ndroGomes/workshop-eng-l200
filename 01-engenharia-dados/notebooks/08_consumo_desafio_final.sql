-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Módulo 08 — Consumo, orquestração e desafio final
-- MAGIC
-- MAGIC O pipeline está pronto. Este módulo tem três partes:
-- MAGIC
-- MAGIC 1. **Consumo** — a gold respondendo perguntas de negócio.
-- MAGIC 2. **Orquestração** — como isso roda sozinho todo mês (Job vs Pipeline).
-- MAGIC 3. **Desafio final** — a pergunta que o CFO fez, sem SQL pronto.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parte 1 — Consumo
-- MAGIC
-- MAGIC ### Célula pronta — o custo assistencial mês a mês
-- MAGIC A mesma pergunta do módulo 00, agora com dado confiável por trás.

-- COMMAND ----------

SELECT
  NU_COMPETENCIA,
  COUNT(DISTINCT NU_PRESTADOR)              AS prestadores,
  SUM(QT_CONTAS)                            AS contas,
  ROUND(SUM(VL_CUSTO_TOTAL) / 1000000, 2)   AS custo_milhoes,
  ROUND(100 * SUM(VL_GLOSA_TOTAL) / SUM(VL_APRESENTADO_TOTAL), 2) AS pct_glosa
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
GROUP BY ALL
ORDER BY NU_COMPETENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Célula pronta — quem está fora do padrão
-- MAGIC O `FL_ANOMALIA_CUSTO` sinalizou prestadores caros **em relação aos pares da
-- MAGIC mesma especialidade**. Estes são os candidatos a auditoria de conta médica.

-- COMMAND ----------

SELECT
  NU_PRESTADOR, NM_PRESTADOR, NM_ESPECIALIDADE, NM_REGIAO,
  NU_COMPETENCIA, QT_CONTAS,
  ROUND(VL_CUSTO_TOTAL, 2)  AS custo,
  IDX_CUSTO_TABELA, IDX_CUSTO_VS_PARES
FROM IDENTIFIER(meu_schema || '.gold_custo_utilizacao_prestador')
WHERE FL_ANOMALIA_CUSTO = 1
ORDER BY IDX_CUSTO_VS_PARES DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Célula pronta — custo por plano e faixa etária
-- MAGIC A gold tem grão **prestador × competência** e por isso **não** responde esta
-- MAGIC pergunta. Ela se responde na silver, juntando a fato com a dimensão de
-- MAGIC beneficiário — exatamente o motivo pelo qual não sobrecarregamos a gold.

-- COMMAND ----------

SELECT
  bp.NM_PLANO,
  bp.NU_FAIXA_ETARIA,
  COUNT(*)                                 AS contas,
  ROUND(SUM(c.VL_PAGO) / 1000, 2)          AS custo_mil,
  ROUND(SUM(c.VL_PAGO) / COUNT(DISTINCT c.NU_BENEFICIARIO), 2) AS custo_por_beneficiario
FROM IDENTIFIER(meu_schema || '.slv_conta_medica') c
INNER JOIN IDENTIFIER(meu_schema || '.slv_beneficiario_plano') bp
        ON bp.NU_BENEFICIARIO = c.NU_BENEFICIARIO
GROUP BY ALL
ORDER BY custo_por_beneficiario DESC
LIMIT 15;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Parte 2 — Orquestração: Job ou Pipeline?
-- MAGIC
-- MAGIC | | **Lakeflow Job** (imperativo) | **Lakeflow Declarative Pipeline** |
-- MAGIC |---|---|---|
-- MAGIC | O que você declara | tarefas e dependências entre elas | o que cada tabela é |
-- MAGIC | Ordem | você desenha o grafo | deduzida pelas referências `LIVE.` |
-- MAGIC | Conteúdo das tarefas | qualquer coisa (SQL, Python, dbt, outro Job) | só definições de tabela/view |
-- MAGIC | Qualidade | você implementa (módulo 06) | `CONSTRAINT ... EXPECT` nativo |
-- MAGIC | Linhas rejeitadas | você decide o destino (quarentena) | descartadas e contadas |
-- MAGIC | Quando usar | fluxo com passos heterogêneos, chamadas externas, controle fino | transformação declarativa de dados ponta a ponta |
-- MAGIC
-- MAGIC Na prática os dois convivem: é comum um **Job** orquestrar um **Pipeline** como
-- MAGIC uma de suas tarefas, com a porta de qualidade do módulo 06 antes da publicação.
-- MAGIC
-- MAGIC ### Como criar o Job deste workshop
-- MAGIC 1. **Lakeflow → Jobs → Create job**.
-- MAGIC 2. Crie uma tarefa por notebook, tipo **Notebook**, na ordem
-- MAGIC    `01 → 02 → 03 → 04 → 05 → 06`, cada uma **depende da anterior**
-- MAGIC    (campo *Depends on*).
-- MAGIC 3. As tarefas `02` (silver dimensões) e `03`/`04` podem rodar em paralelo?
-- MAGIC    Olhe as dependências reais de tabela antes de responder — é o mesmo
-- MAGIC    raciocínio que o Lakeflow fez sozinho no módulo 07.
-- MAGIC 4. Rode com **Run now** e acompanhe o grafo de tarefas.
-- MAGIC
-- MAGIC 💡 Não é preciso executar o Job para concluir o workshop; se o tempo estiver
-- MAGIC curto, o instrutor demonstra e a turma vai direto ao desafio.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Parte 3 — ⭐ Desafio final
-- MAGIC
-- MAGIC > **De:** Diretoria Financeira
-- MAGIC > **Assunto:** custo assistencial do 4º trimestre
-- MAGIC >
-- MAGIC > _"O custo assistencial fechou o trimestre bem acima do orçado e ninguém
-- MAGIC > consegue me explicar por quê. Não quero saber que 'aumentou'. Quero saber
-- MAGIC > **onde** aumentou, **quanto** disso é explicado por cada causa e **o que
-- MAGIC > fazer** na semana que vem."_
-- MAGIC
-- MAGIC ## Sua missão
-- MAGIC Usando as tabelas que **você mesmo construiu**, monte uma resposta que:
-- MAGIC
-- MAGIC 1. **quantifique** a variação de custo entre o início e o fim da série,
-- MAGIC    separando o que é **volume** (mais contas) do que é **preço**
-- MAGIC    (mais custo pela mesma coisa);
-- MAGIC 2. identifique **pelo menos duas causas distintas** com naturezas diferentes —
-- MAGIC    uma concentrada em poucos prestadores, outra difusa;
-- MAGIC 3. diga **quanto** cada causa custou em reais;
-- MAGIC 4. termine com uma **recomendação acionável por causa**.
-- MAGIC
-- MAGIC ## Tabelas disponíveis
-- MAGIC | Tabela | Grão |
-- MAGIC |---|---|
-- MAGIC | `gold_custo_utilizacao_prestador` | prestador × competência |
-- MAGIC | `slv_conta_medica` | conta médica (guia) |
-- MAGIC | `slv_prestador_estabelecimento` | prestador (estado atual) |
-- MAGIC | `slv_prestador_vigencia` | prestador × vigência |
-- MAGIC | `slv_beneficiario_plano` | beneficiário |
-- MAGIC | `qua_conta_invalida`, `qua_prestador_orfao` | quarentenas |
-- MAGIC | `dq_metricas` | painel de qualidade |
-- MAGIC
-- MAGIC ## Critérios de sucesso
-- MAGIC - [ ] A variação de custo está **decomposta**, não apenas medida.
-- MAGIC - [ ] Duas causas de naturezas diferentes, cada uma com valor em R$.
-- MAGIC - [ ] Pelo menos uma causa **não** é encontrável pelo `FL_ANOMALIA_CUSTO`.
-- MAGIC - [ ] As conclusões usam `IDX_CUSTO_TABELA` (ou outra métrica insensível ao mix),
-- MAGIC       não custo médio por conta puro.
-- MAGIC - [ ] Você checou o painel de qualidade antes de confiar nos números.
-- MAGIC - [ ] Uma recomendação por causa, endereçada a uma área.
-- MAGIC
-- MAGIC ## Dicas (abra só se travar)
-- MAGIC <details><summary>Dica 1 — por onde começar</summary>
-- MAGIC
-- MAGIC Compare o custo por **região × competência**. A média nacional esconde
-- MAGIC comportamento regional.
-- MAGIC </details>
-- MAGIC
-- MAGIC <details><summary>Dica 2 — separar volume de preço</summary>
-- MAGIC
-- MAGIC `SUM(VL_CUSTO_TOTAL)` sobe por dois motivos: mais contas ou custo maior por
-- MAGIC conta. `SUM(VL_REFERENCIA_TOTAL)` sobe **só** com volume e mix. Divida um pelo
-- MAGIC outro.
-- MAGIC </details>
-- MAGIC
-- MAGIC <details><summary>Dica 3 — a causa difusa</summary>
-- MAGIC
-- MAGIC O `FL_ANOMALIA_CUSTO` compara cada prestador com os **pares da sua
-- MAGIC especialidade**. Se um grupo inteiro subir junto, a média dos pares sobe com ele
-- MAGIC e ninguém é sinalizado. Que dimensão agrupa prestadores que sobem juntos?
-- MAGIC </details>
-- MAGIC
-- MAGIC <details><summary>Dica 4 — quantificar em reais</summary>
-- MAGIC
-- MAGIC "Custo excedente" = custo observado − custo que teria ocorrido com o índice do
-- MAGIC período-base: `VL_REFERENCIA_TOTAL * (IDX_observado - IDX_base)`.
-- MAGIC </details>
-- MAGIC
-- MAGIC ## Entrega
-- MAGIC 3 a 5 queries + um parágrafo curto de conclusão. Escreva a conclusão numa célula
-- MAGIC `%md` como se fosse o e-mail de resposta à Diretoria.

-- COMMAND ----------

-- 👉 Desafio final: escreva suas queries a partir daqui.
-- Use o Databricks Assistant, mas a pergunta é sua: descreva a análise, não o SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. A turma achou as **duas** naturezas de causa, ou parou na primeira?
-- MAGIC 2. Alguém usou custo médio por conta e chegou a uma conclusão errada? Ótimo
-- MAGIC    momento para retomar a discussão do módulo 05.
-- MAGIC 3. Alguém conferiu `dq_metricas` antes de responder ao CFO?
