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
-- MAGIC # Módulo 00 — Contexto de Negócio e Exploração dos Dados
-- MAGIC
-- MAGIC ## O problema de negócio
-- MAGIC
-- MAGIC Você faz parte do time de dados de uma **operadora de saúde** (Amil). A área de
-- MAGIC **Gestão de Rede e Custo Assistencial** chegou com um problema concreto:
-- MAGIC
-- MAGIC > _"O custo assistencial do último trimestre subiu bem acima do esperado.
-- MAGIC > Achamos que existem prestadores cobrando fora do padrão, mas cada área olha
-- MAGIC > um sistema diferente e ninguém consegue provar nada. Precisamos de **um**
-- MAGIC > lugar confiável que mostre custo e utilização por prestador, por mês."_
-- MAGIC
-- MAGIC Os dados estão espalhados em três sistemas de origem:
-- MAGIC
-- MAGIC | Sistema | O que controla | Tabelas de landing |
-- MAGIC |---|---|---|
-- MAGIC | **SGR** — Sistema de Gestão da Rede | rede credenciada | `raw_sgr_tb_estabelecimento`, `raw_sgr_tb_prestador`, `raw_sgr_au_prestador` |
-- MAGIC | **SGB** — Sistema de Gestão de Beneficiários | carteira e planos | `raw_sgb_tb_plano`, `raw_sgb_tb_beneficiario` |
-- MAGIC | **SIA** — Sistema de Informações Assistenciais | contas médicas (guias) | `raw_sia_tb_procedimento`, `raw_sia_tb_conta_medica` |
-- MAGIC
-- MAGIC Ao longo do workshop você vai levar esses dados de **landing → bronze → silver
-- MAGIC → gold** e entregar a tabela que responde a pergunta da área de negócio.
-- MAGIC
-- MAGIC > ⚠️ **Todos os dados utilizados no workshop são sintéticos e não representam
-- MAGIC > pacientes reais.** Não há nome, CPF, endereço ou dado clínico em nenhuma
-- MAGIC > tabela — apenas identificadores numéricos gerados por código.
-- MAGIC
-- MAGIC ## Como usar o Databricks Assistant neste workshop
-- MAGIC Em vez de digitar SQL na mão, você vai **descrever em português** o que quer
-- MAGIC e deixar o **Databricks Assistant** gerar o SQL:
-- MAGIC
-- MAGIC 1. Em uma célula vazia, clique no ícone do **Assistant** (✨) ou pressione
-- MAGIC    `Cmd/Ctrl + I`.
-- MAGIC 2. Escreva o prompt (em português) e gere o código.
-- MAGIC 3. **Revise** o SQL gerado, rode e confira o resultado.
-- MAGIC
-- MAGIC > Nos blocos **PROMPT** abaixo está o texto sugerido. Ajuste como quiser.
-- MAGIC > O Assistant acelera a escrita; a **revisão crítica** continua sendo sua.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Seu schema pessoal
-- MAGIC Cada participante trabalha em um schema próprio dentro do catálogo
-- MAGIC `amil_workshop_trilha_tech`. A variável abaixo resolve o seu schema a partir
-- MAGIC do seu usuário — assim o mesmo código funciona para todo mundo, sem edição.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');

-- COMMAND ----------

SELECT meu_schema AS schema_de_trabalho;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — volume das 7 tabelas de landing
-- MAGIC Rode para ver o tamanho de cada origem gerada no notebook de setup.

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_estabelecimento')) AS estabelecimentos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador'))       AS prestadores,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgr_au_prestador'))       AS eventos_auditoria,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_plano'))           AS planos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sgb_tb_beneficiario'))    AS beneficiarios,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sia_tb_procedimento'))    AS procedimentos,
  (SELECT COUNT(*) FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica'))    AS contas_medicas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — como os dados se ligam
-- MAGIC
-- MAGIC ```
-- MAGIC raw_sgr_tb_estabelecimento (CD_ESTABELECIMENTO)
-- MAGIC         │ 1
-- MAGIC         │
-- MAGIC         │ N
-- MAGIC raw_sgr_tb_prestador (NU_PRESTADOR) ──N── raw_sgr_au_prestador (histórico)
-- MAGIC         │ 1
-- MAGIC         │
-- MAGIC         │ N
-- MAGIC raw_sia_tb_conta_medica (NU_GUIA)  ──N──1── raw_sia_tb_procedimento (CD_PROCEDIMENTO)
-- MAGIC         │ N
-- MAGIC         │
-- MAGIC         │ 1
-- MAGIC raw_sgb_tb_beneficiario (NU_BENEFICIARIO) ──N──1── raw_sgb_tb_plano (CD_PLANO)
-- MAGIC ```
-- MAGIC
-- MAGIC A conta médica é o **fato**: uma linha por guia de atendimento, ligando
-- MAGIC beneficiário, prestador e procedimento em uma competência (mês).

-- COMMAND ----------

SELECT NU_GUIA, NU_BENEFICIARIO, NU_PRESTADOR, CD_PROCEDIMENTO,
       DT_ATENDIMENTO, NU_COMPETENCIA, QT_ITEM, VL_APRESENTADO, VL_GLOSA, VL_PAGO
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica')
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Célula pronta — evolução do custo por competência
-- MAGIC Este é o sintoma que a área de negócio relatou. Rode e observe o último trimestre.

-- COMMAND ----------

SELECT
  CAST(NU_COMPETENCIA AS INT)               AS competencia,
  COUNT(*)                                  AS contas,
  ROUND(SUM(VL_PAGO) / 1000000, 2)          AS custo_milhoes
FROM IDENTIFIER(meu_schema || '.raw_sia_tb_conta_medica')
GROUP BY ALL
ORDER BY competencia;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ⭐ Exercício-chave (com o Assistant) — Integridade referencial
-- MAGIC
-- MAGIC Antes de confiar em qualquer número, precisamos saber se as chaves fecham.
-- MAGIC
-- MAGIC **PROMPT sugerido para o Assistant:**
-- MAGIC > _"Conte quantos prestadores da tabela
-- MAGIC > IDENTIFIER(meu_schema || '.raw_sgr_tb_prestador') não têm estabelecimento
-- MAGIC > correspondente em IDENTIFIER(meu_schema || '.raw_sgr_tb_estabelecimento'),
-- MAGIC > usando LEFT JOIN por CD_ESTABELECIMENTO e contando onde o estabelecimento
-- MAGIC > é nulo. Faça o mesmo para contas médicas sem prestador correspondente."_
-- MAGIC
-- MAGIC Esses são os registros **órfãos**. Eles não podem simplesmente desaparecer:
-- MAGIC no módulo 02 vamos mandá-los para uma tabela de **quarentena**.
-- MAGIC
-- MAGIC Gere o SQL com o Assistant na célula abaixo, rode e observe o resultado.

-- COMMAND ----------

-- 👉 Gere o SQL aqui com o Databricks Assistant (Cmd/Ctrl + I) usando o prompt acima.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Checkpoint do instrutor
-- MAGIC Antes de seguir, você deve conseguir responder:
-- MAGIC
-- MAGIC 1. Qual tabela é o **fato** e qual é a **dimensão**?
-- MAGIC 2. Em qual competência o custo sai do padrão?
-- MAGIC 3. Por que descartar os órfãos em silêncio é uma má ideia?
