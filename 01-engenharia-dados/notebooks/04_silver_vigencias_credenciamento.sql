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
-- MAGIC # Módulo 04 — Silver: vigências do credenciamento (SCD Tipo 2)
-- MAGIC
-- MAGIC `slv_prestador_estabelecimento` responde _"como o prestador é **hoje**"_.
-- MAGIC Mas a conta médica de março/2025 precisa ser lida com a especialidade e o
-- MAGIC status que o prestador tinha **em março de 2025** — não com os de hoje.
-- MAGIC
-- MAGIC Se um prestador de Clínica Médica mudou para Oncologia em agosto, atribuir
-- MAGIC todas as contas do ano à Oncologia distorce o custo médio das duas
-- MAGIC especialidades. É assim que um relatório "certo" mente.
-- MAGIC
-- MAGIC A tabela `raw_sgr_au_prestador` guarda cada **evento de alteração cadastral**.
-- MAGIC Nosso trabalho é transformar eventos em **intervalos de vigência**:
-- MAGIC
-- MAGIC ```
-- MAGIC eventos:     |----X--------X------------X------->   (datas de alteração)
-- MAGIC vigências:   [ v1 ][  v2  ][    v3     ][  v4  ]    (intervalos contíguos)
-- MAGIC ```
-- MAGIC
-- MAGIC **Três armadilhas neste módulo:**
-- MAGIC 1. precedência de `AND`/`OR` no filtro da auditoria;
-- MAGIC 2. eventos com atributos nulos, que precisam herdar o último valor conhecido;
-- MAGIC 3. preencher para **frente** e não para trás — nunca trazer informação do futuro.

-- COMMAND ----------

-- 👇 TROQUE `seu_usuario` pelo nome do schema que o setup criou para você.
--    É o seu e-mail antes do @, com o ponto trocado por underscore.
--    Ex.: maria.silva@amil.com.br  ->  maria_silva
USE CATALOG amil_workshop_trilha_tech;
USE SCHEMA seu_usuario;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — o que tem de errado na auditoria
-- MAGIC A origem grava eventos **estornados** (`FL_EXCLUIDO = 1`), eventos
-- MAGIC **completamente vazios** (bug do sistema) e eventos com `NU_UNIDADE` nulo.

-- COMMAND ----------

SELECT
  COUNT(*)                                                          AS eventos,
  SUM(CASE WHEN CAST(FL_EXCLUIDO AS INT) = 1 THEN 1 ELSE 0 END)     AS estornados,
  SUM(CASE WHEN FL_STATUS_PRESTADOR IS NULL
             AND CD_ESPECIALIDADE  IS NULL
             AND NU_UNIDADE        IS NULL
             AND DT_CREDENCIAMENTO IS NULL THEN 1 ELSE 0 END)       AS eventos_vazios,
  SUM(CASE WHEN NU_UNIDADE IS NULL THEN 1 ELSE 0 END)               AS sem_unidade
FROM brz_prestador_auditoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — a armadilha de precedência (rode e compare!)
-- MAGIC
-- MAGIC Queremos: eventos **não estornados** **e** que tenham **algum** atributo
-- MAGIC preenchido. As duas versões abaixo parecem iguais. Não são.
-- MAGIC
-- MAGIC Em SQL, `AND` tem precedência **maior** que `OR`. Então
-- MAGIC `A AND B OR C OR D` é lido como `(A AND B) OR C OR D` — e o filtro `A`
-- MAGIC (`FL_EXCLUIDO = 0`) simplesmente **deixa de valer** para as linhas que
-- MAGIC satisfazem `C` ou `D`.

-- COMMAND ----------

SELECT
  -- ERRADO: sem parênteses, o FL_EXCLUIDO é anulado pelos ORs
  COUNT_IF(CAST(FL_EXCLUIDO AS INT) = 0
           AND FL_STATUS_PRESTADOR IS NOT NULL
           OR  CD_ESPECIALIDADE  IS NOT NULL
           OR  NU_UNIDADE        IS NOT NULL
           OR  DT_CREDENCIAMENTO IS NOT NULL)                      AS sem_parenteses,
  -- CORRETO: o bloco de ORs entre parênteses
  COUNT_IF(CAST(FL_EXCLUIDO AS INT) = 0
           AND (FL_STATUS_PRESTADOR IS NOT NULL
                OR CD_ESPECIALIDADE  IS NOT NULL
                OR NU_UNIDADE        IS NOT NULL
                OR DT_CREDENCIAMENTO IS NOT NULL))                 AS com_parenteses
FROM brz_prestador_auditoria;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 1 (com o Assistant) — unificar histórico + estado atual
-- MAGIC
-- MAGIC A auditoria tem o passado; `slv_prestador_estabelecimento` tem o presente.
-- MAGIC A linha do tempo completa é a união dos dois.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela slv_prestador_evento com um
-- MAGIC > UNION ALL de duas partes. Primeira parte: de
-- MAGIC > brz_prestador_auditoria, com CAST de
-- MAGIC > NU_PRESTADOR e CD_ESTABELECIMENTO para BIGINT, FL_STATUS_PRESTADOR,
-- MAGIC > CD_ESPECIALIDADE, NU_UNIDADE, CD_MOTIVO_DESCREDENCIAMENTO e
-- MAGIC > NU_CAPACIDADE_ATEND_MES para INT, DT_CREDENCIAMENTO, DT_DESCREDENCIAMENTO e
-- MAGIC > DT_AUDIT para DATE; filtrando FL_EXCLUIDO = 0 E, entre parênteses, pelo menos
-- MAGIC > um entre FL_STATUS_PRESTADOR, CD_ESPECIALIDADE, NU_UNIDADE ou
-- MAGIC > DT_CREDENCIAMENTO não nulo. Segunda parte: as mesmas colunas de
-- MAGIC > slv_prestador_estabelecimento, usando
-- MAGIC > CURRENT_DATE() como DT_AUDIT."_
-- MAGIC
-- MAGIC ⚠️ Os parênteses são obrigatórios — veja a célula anterior.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício 2 (com o Assistant) — calcular as vigências (SCD2)
-- MAGIC
-- MAGIC Cada evento fecha a vigência anterior e abre a sua:
-- MAGIC
-- MAGIC - `DT_FIM_VIGENCIA` = data do evento (`DT_AUDIT`);
-- MAGIC - `DT_INICIO_VIGENCIA` = data do evento **anterior** (`LAG`), ou o
-- MAGIC   credenciamento no primeiro evento;
-- MAGIC - atributo nulo herda **o último valor conhecido antes dele**.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Crie a tabela slv_prestador_vigencia a partir
-- MAGIC > de slv_prestador_evento. Em uma CTE, calcule
-- MAGIC > DT_FIM_VIGENCIA = DT_AUDIT e DT_INICIO_VIGENCIA =
-- MAGIC > COALESCE(LAG(DT_AUDIT) OVER (PARTITION BY NU_PRESTADOR ORDER BY DT_AUDIT),
-- MAGIC > DT_CREDENCIAMENTO, DT_AUDIT). Em uma segunda CTE, preencha os atributos
-- MAGIC > nulos com LAST_VALUE(coluna, true) sobre a janela
-- MAGIC > PARTITION BY NU_PRESTADOR ORDER BY DT_FIM_VIGENCIA ASC ROWS BETWEEN
-- MAGIC > UNBOUNDED PRECEDING AND CURRENT ROW, aplicando isso a CD_ESTABELECIMENTO,
-- MAGIC > FL_STATUS_PRESTADOR, CD_ESPECIALIDADE, NU_UNIDADE, NU_CAPACIDADE_ATEND_MES e
-- MAGIC > DT_CREDENCIAMENTO. No SELECT final, mantenha apenas as linhas em que
-- MAGIC > DT_FIM_VIGENCIA > DT_INICIO_VIGENCIA."_
-- MAGIC
-- MAGIC 💡 **Por que `LAST_VALUE` com `ORDER BY ... ASC` e não `FIRST_VALUE` com
-- MAGIC `DESC`?** Porque a janela vai de `UNBOUNDED PRECEDING` até `CURRENT ROW`.
-- MAGIC Com `ASC`, "precedente" significa *passado* e o preenchimento é para frente
-- MAGIC (correto). Com `DESC`, "precedente" significa *futuro*: toda linha receberia o
-- MAGIC valor mais recente do prestador, apagando o histórico e vazando informação que
-- MAGIC ainda não existia naquela data.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 1 — volume e contiguidade

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM slv_prestador_evento)            AS eventos,
  (SELECT COUNT(*) FROM slv_prestador_vigencia)          AS vigencias,
  (SELECT COUNT(DISTINCT NU_PRESTADOR) FROM slv_prestador_vigencia) AS prestadores;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 2 — nenhuma vigência pode se sobrepor
-- MAGIC Esta query deve retornar **zero linhas**. Se retornar, o `LAG` está errado.

-- COMMAND ----------

SELECT NU_PRESTADOR, DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA, fim_anterior
FROM (
  SELECT NU_PRESTADOR, DT_INICIO_VIGENCIA, DT_FIM_VIGENCIA,
         LAG(DT_FIM_VIGENCIA) OVER (PARTITION BY NU_PRESTADOR ORDER BY DT_INICIO_VIGENCIA) AS fim_anterior
  FROM slv_prestador_vigencia
)
WHERE fim_anterior > DT_INICIO_VIGENCIA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência 3 — o histórico realmente varia?
-- MAGIC Se o preenchimento tiver sido feito para trás (`DESC`), a especialidade ficará
-- MAGIC **constante** em todos os prestadores e esta query mostrará sempre `1`.

-- COMMAND ----------

SELECT
  qt_especialidades_distintas,
  COUNT(*) AS prestadores
FROM (
  SELECT NU_PRESTADOR, COUNT(DISTINCT CD_ESPECIALIDADE) AS qt_especialidades_distintas
  FROM slv_prestador_vigencia
  GROUP BY NU_PRESTADOR
)
GROUP BY ALL
ORDER BY qt_especialidades_distintas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC 1. O que acontece com um prestador que tem **um único** evento na auditoria?
-- MAGIC 2. Por que descartamos as vigências com `DT_FIM <= DT_INICIO`?
-- MAGIC 3. Em qual coluna desta tabela a conta médica vai "pousar" no módulo 05?
