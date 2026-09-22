# Databricks notebook source
# MAGIC %md
# MAGIC # Workshop Amil x Databricks — Setup do Participante
# MAGIC
# MAGIC Este notebook prepara o ambiente **individual** de cada participante.
# MAGIC Rode-o **uma única vez** no início do workshop.
# MAGIC
# MAGIC O que ele faz:
# MAGIC 1. Descobre o seu usuário automaticamente.
# MAGIC 2. Cria o seu schema pessoal em `amil_workshop_trilha_tech.<seu_usuario>`.
# MAGIC 3. **Gera os seus próprios dados sintéticos** dentro do seu schema, simulando
# MAGIC    a extração de **três sistemas operacionais** da operadora:
# MAGIC    - **SGR** — Sistema de Gestão da Rede Credenciada (`raw_sgr_*`)
# MAGIC    - **SGB** — Sistema de Gestão de Beneficiários (`raw_sgb_*`)
# MAGIC    - **SIA** — Sistema de Informações Assistenciais / contas médicas (`raw_sia_*`)
# MAGIC 4. Confere que os dados foram criados corretamente.
# MAGIC
# MAGIC > ### ⚠️ Privacidade / LGPD
# MAGIC > **Todos os dados utilizados no workshop são sintéticos e não representam
# MAGIC > pacientes, prestadores ou contratos reais.** Nada aqui vem de sistema de
# MAGIC > produção. Por decisão de projeto, o cadastro de beneficiários **não possui
# MAGIC > nome, CPF, endereço ou qualquer dado clínico identificável** — trabalhamos
# MAGIC > apenas com códigos, faixa etária, sexo e região. Essa é a postura esperada
# MAGIC > de uma camada analítica de operadora de saúde.
# MAGIC
# MAGIC > Não há fonte central compartilhada: **cada participante gera e trabalha com
# MAGIC > os seus próprios dados**, todos dentro de `amil_workshop_trilha_tech.<seu_usuario>`.
# MAGIC > A geração é **determinística** (usa `hash()`/`pmod()`, nunca `rand()`), então
# MAGIC > todos obtêm o mesmo resultado — o que permite comparar com o gabarito.
# MAGIC >
# MAGIC > A **exploração** desses dados é o **próximo passo** (módulo 00).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Identificação do participante

# COMMAND ----------

# O e-mail do usuário logado vira o nome do schema (sanitizado).
raw_user = spark.sql("SELECT current_user() AS u").first()["u"]

# Ex.: "maria.silva@amil.com.br" -> "maria_silva"
username = raw_user.split("@")[0].replace(".", "_").replace("-", "_").lower()

print(f"Usuário logado : {raw_user}")
print(f"Schema pessoal : {username}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers de identificador
# MAGIC
# MAGIC Funções para montar nomes totalmente qualificados e sanitizados com crases
# MAGIC (backticks), evitando problemas com caracteres especiais.

# COMMAND ----------


def quote_identifier(value: str) -> str:
    """Sanitiza um identificador com crases (backticks)."""
    return f"`{value.replace('`', '``')}`"


CATALOG = "amil_workshop_trilha_tech"

catalog_q = quote_identifier(CATALOG)
schema_q = quote_identifier(username)
target_schema = f"{catalog_q}.{schema_q}"


def table_name(name: str) -> str:
    """Nome de tabela totalmente qualificado dentro do schema do participante."""
    return f"{target_schema}.{quote_identifier(name)}"


print(f"Schema de trabalho: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criação do catálogo e do schema pessoal
# MAGIC
# MAGIC No workshop, o catálogo `amil_workshop_trilha_tech` normalmente **já existe**
# MAGIC (criado pelo instrutor). Se não existir, a célula abaixo tenta criá-lo.
# MAGIC
# MAGIC Sem permissão de `CREATE CATALOG`? Rode com um catálogo que você já tenha,
# MAGIC descomentando e ajustando a linha `CATALOG = ...` abaixo — nada mais no
# MAGIC workshop muda, porque todos os notebooks derivam o schema desta variável.

# COMMAND ----------

# Para usar um catálogo já existente (ex.: workspace, main, sandbox), descomente:
# CATALOG = "workspace"
# catalog_q     = quote_identifier(CATALOG)
# target_schema = f"{catalog_q}.{schema_q}"

catalogos = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]

if CATALOG not in catalogos:
    print(f"Catálogo '{CATALOG}' não existe. Tentando criar...")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_q}")
        print(f"Catálogo criado: {CATALOG}")
    except Exception as erro:
        raise RuntimeError(
            f"Não foi possível criar o catálogo '{CATALOG}': {erro}\n\n"
            "Duas saídas:\n"
            "  1) peça ao administrador do workspace um catálogo com permissão de "
            "CREATE SCHEMA/CREATE TABLE; ou\n"
            "  2) descomente a linha 'CATALOG = ...' no topo desta célula, apontando "
            f"para um catálogo que você já usa. Disponíveis aqui: {catalogos}\n\n"
            "Depois, lembre-se de usar o MESMO catálogo no DECLARE de meu_schema "
            "nos notebooks dos módulos."
        ) from erro

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
print(f"Schema pronto: {target_schema}")
print(
    "\nNos notebooks dos módulos, use exatamente:\n"
    f"  DECLARE OR REPLACE VARIABLE meu_schema STRING DEFAULT '{CATALOG}.{username}';"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Geração dos dados sintéticos no SEU schema
# MAGIC
# MAGIC Criamos 7 tabelas de **landing**, como se tivessem acabado de ser extraídas
# MAGIC dos sistemas de origem — com tipos "crus" (`DECIMAL(38,10)`, `TIMESTAMP`) e
# MAGIC com **defeitos propositais** de qualidade, que são a matéria-prima dos
# MAGIC exercícios.
# MAGIC
# MAGIC | Sistema | Tabela | Conteúdo |
# MAGIC |---|---|---|
# MAGIC | SGR | `raw_sgr_tb_estabelecimento` | Estabelecimentos de saúde da rede (hospitais, clínicas, laboratórios) |
# MAGIC | SGR | `raw_sgr_tb_prestador` | Prestadores credenciados (estado atual) |
# MAGIC | SGR | `raw_sgr_au_prestador` | Auditoria do credenciamento (histórico de alterações) |
# MAGIC | SGB | `raw_sgb_tb_plano` | Planos comercializados |
# MAGIC | SGB | `raw_sgb_tb_beneficiario` | Carteira de beneficiários (sem dados identificáveis) |
# MAGIC | SIA | `raw_sia_tb_procedimento` | Catálogo de procedimentos (padrão tipo TUSS) |
# MAGIC | SIA | `raw_sia_tb_conta_medica` | Contas médicas / guias de atendimento |
# MAGIC
# MAGIC **Volumes (otimizados para o tempo do workshop, não para realismo):**
# MAGIC 12 estabelecimentos, 600 prestadores, ~1.500 eventos de auditoria, 6 planos,
# MAGIC 20.000 beneficiários, 200 procedimentos, ~122.000 contas médicas
# MAGIC (12 competências: 2025-01 a 2025-12).

# COMMAND ----------

estab_tbl = table_name("raw_sgr_tb_estabelecimento")
prest_tbl = table_name("raw_sgr_tb_prestador")
audit_tbl = table_name("raw_sgr_au_prestador")
plano_tbl = table_name("raw_sgb_tb_plano")
benef_tbl = table_name("raw_sgb_tb_beneficiario")
proc_tbl = table_name("raw_sia_tb_procedimento")
conta_tbl = table_name("raw_sia_tb_conta_medica")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 SGR — Estabelecimentos (cadastro "pai") — `CD_ESTABELECIMENTO` é a chave do join
# MAGIC
# MAGIC Tipos propositalmente "crus" (`DECIMAL(38,10)`/`TIMESTAMP`) para exercitar a
# MAGIC tipagem na camada silver.
# MAGIC
# MAGIC **Defeitos propositais:**
# MAGIC - **versões duplicadas** do mesmo estabelecimento (`dt_carga_bronze` diferente)
# MAGIC   → exercício de deduplicação com `QUALIFY`;
# MAGIC - duas colunas de documento: `NU_CNPJ` (preenchida) e `NU_INSCRICAO_MUNICIPAL`
# MAGIC   (vazia, *decoy*) → o participante precisa conferir qual usar.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {estab_tbl} AS
SELECT
  CAST(CD_ESTABELECIMENTO AS DECIMAL(38,10))                                          AS CD_ESTABELECIMENTO,
  NM_ESTABELECIMENTO                                                                  AS NM_ESTABELECIMENTO,
  CAST(CD_TIPO_ESTABELECIMENTO AS DECIMAL(38,10))                                     AS CD_TIPO_ESTABELECIMENTO,
  SG_UF                                                                               AS SG_UF,
  NM_MUNICIPIO                                                                        AS NM_MUNICIPIO,
  NM_REGIAO                                                                           AS NM_REGIAO,
  CAST(TIMESTAMP(DATE_ADD(DATE'1975-01-01', pmod(hash(CD_ESTABELECIMENTO,'ini'), 16000))) AS TIMESTAMP) AS DT_INICIO_OPERACAO,
  CAST(NU_LEITOS AS DECIMAL(38,10))                                                   AS NU_LEITOS,
  -- Documento "oficial" preenchido; a coluna decoy (NU_INSCRICAO_MUNICIPAL) vem
  -- vazia de propósito para o participante aprender a conferir a coluna certa.
  CAST(30000000000000 + CD_ESTABELECIMENTO * 137 AS DECIMAL(38,0))                    AS NU_CNPJ,
  CAST(NULL AS STRING)                                                                AS NU_INSCRICAO_MUNICIPAL,
  CAST(0 AS DECIMAL(38,10))                                                           AS FL_EXCLUIDO,
  CONCAT(CAST(CD_ESTABELECIMENTO AS STRING), '_v', CAST(versao AS STRING))             AS merge_key,
  -- versões mais novas têm dt_carga_bronze maior (vencem no QUALIFY)
  TIMESTAMPADD(DAY, versao, TIMESTAMP'2025-01-01')                                    AS dt_carga_bronze
FROM VALUES
  ( 1, 'Hospital Sao Paulo Central',      1, 'SP', 'Sao Paulo',       'Sudeste',      420, 0),
  ( 2, 'Hospital Campinas Norte',         1, 'SP', 'Campinas',        'Sudeste',      180, 0),
  ( 3, 'Laboratorio Paulista Analises',   3, 'SP', 'Sao Paulo',       'Sudeste',        0, 0),
  ( 4, 'Hospital Rio Botafogo',           1, 'RJ', 'Rio de Janeiro',  'Sudeste',      310, 0),
  ( 5, 'Clinica Niteroi Saude',           2, 'RJ', 'Niteroi',         'Sudeste',       25, 0),
  ( 6, 'Hospital Belo Horizonte Savassi', 1, 'MG', 'Belo Horizonte',  'Sudeste',      260, 0),
  ( 7, 'Hospital Curitiba Batel',         1, 'PR', 'Curitiba',        'Sul',          210, 0),
  ( 8, 'Clinica Porto Alegre Moinhos',    2, 'RS', 'Porto Alegre',    'Sul',           40, 0),
  ( 9, 'Hospital Salvador Atlantico',     1, 'BA', 'Salvador',        'Nordeste',     230, 0),
  (10, 'Hospital Recife Boa Viagem',      1, 'PE', 'Recife',          'Nordeste',     190, 0),
  (11, 'Clinica Goiania Bueno',           2, 'GO', 'Goiania',         'Centro-Oeste',  35, 0),
  (12, 'Pronto Atendimento Brasilia Sul', 4, 'DF', 'Brasilia',        'Centro-Oeste',  60, 0),
  -- versões antigas (dedup): mesmo estabelecimento, dt_carga_bronze menor (perdem no QUALIFY)
  ( 1, 'Hospital Sao Paulo Central (ant)',1, 'SP', 'Sao Paulo',       'Sudeste',      400, -60),
  ( 4, 'Hospital Rio Botafogo (ant)',     1, 'RJ', 'Rio de Janeiro',  'Sudeste',      300, -60),
  ( 9, 'Hospital Salvador (ant)',         1, 'BA', 'Salvador',        'Nordeste',     220, -60)
AS t(CD_ESTABELECIMENTO, NM_ESTABELECIMENTO, CD_TIPO_ESTABELECIMENTO, SG_UF, NM_MUNICIPIO, NM_REGIAO, NU_LEITOS, versao)
""")
print(f"OK: {estab_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 SGR — Prestadores (estado atual de cada prestador credenciado)
# MAGIC
# MAGIC `CD_ESTABELECIMENTO` casa com o estabelecimento, **exceto nos órfãos**
# MAGIC (`CD_ESTABELECIMENTO = 9999`, ~2%) → quarentena no módulo 02.
# MAGIC
# MAGIC - `FL_STATUS_PRESTADOR`: **2 = credenciado ativo**, **4 = descredenciado** (~16%)
# MAGIC - `CD_ESPECIALIDADE`: 1 Clínica Médica · 2 Cardiologia · 3 Ortopedia ·
# MAGIC   4 Pediatria · 5 Ginecologia/Obstetrícia · 6 Oncologia ·
# MAGIC   7 Diagnóstico por Imagem · 8 Análises Clínicas
# MAGIC - `CD_TIPO_PRESTADOR`: 1 Hospital · 2 Clínica · 3 Laboratório · 4 Profissional PF
# MAGIC - `NU_UNIDADE`: unidade/ala dentro do estabelecimento (1–4)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {prest_tbl} AS
SELECT
  CAST(70000 + id AS DECIMAL(38,10))                                              AS NU_PRESTADOR,
  -- ~2% dos prestadores apontam para estabelecimento inexistente (órfãos → quarentena).
  -- Os 10 prestadores com anomalia de custo plantada ficam de fora do sorteio de
  -- defeitos cadastrais, para que a resposta do desafio final seja sempre a mesma.
  CAST(CASE WHEN pmod(hash(id,'orf'),50)=0 AND NOT plantado.fl THEN 9999
            ELSE 1 + pmod(hash(id,'estab'),12) END AS DECIMAL(38,10))             AS CD_ESTABELECIMENTO,
  CONCAT('CNES-', LPAD(CAST(id AS STRING), 7, '0'))                               AS CD_CNES,
  CONCAT('PRESTADOR ', LPAD(CAST(id AS STRING), 4, '0'))                          AS NM_PRESTADOR,
  st.status                                                                       AS FL_STATUS_PRESTADOR,
  CAST(1 + pmod(hash(id,'esp'),8) AS DECIMAL(38,10))                              AS CD_ESPECIALIDADE,
  CAST(1 + pmod(hash(id,'tipo'),4) AS DECIMAL(38,10))                             AS CD_TIPO_PRESTADOR,
  CAST(1 + pmod(hash(id,'uni'),4) AS DECIMAL(38,10))                              AS NU_UNIDADE,
  cred.dt                                                                         AS DT_CREDENCIAMENTO,
  -- descredenciamentos concentrados a partir de 2025-07 (janela relevante p/ o workshop)
  CASE WHEN st.status = 4
       THEN CAST(TIMESTAMP(DATE_ADD(DATE'2025-07-01', pmod(hash(id,'desc'), 700))) AS TIMESTAMP) END AS DT_DESCREDENCIAMENTO,
  CASE WHEN st.status = 4
       THEN CAST(1 + pmod(hash(id,'mot'),6) AS DECIMAL(38,10)) END                AS CD_MOTIVO_DESCREDENCIAMENTO,
  -- capacidade contratada de atendimentos por mês (100 a 999)
  CAST(100 + pmod(hash(id,'cap'),900) AS DECIMAL(38,10))                          AS NU_CAPACIDADE_ATEND_MES,
  CAST(CASE WHEN pmod(hash(id,'exc'),60)=0 AND NOT plantado.fl THEN 1
            ELSE 0 END AS DECIMAL(38,10))                                         AS FL_EXCLUIDO,
  CAST(70000 + id AS STRING)                                                      AS merge_key,
  CURRENT_TIMESTAMP()                                                             AS dt_carga_bronze
FROM (SELECT explode(sequence(1, 600)) AS id) g
  CROSS JOIN LATERAL (
    -- prestadores com anomalia de custo plantada (resposta do desafio final)
    SELECT array_contains(array(7,23,41,88,152,246,333,417,501,588), g.id) AS fl
  ) plantado
  CROSS JOIN LATERAL (SELECT CAST(TIMESTAMP(DATE_ADD(DATE'2005-01-01', pmod(hash(g.id,'cred'), 6200))) AS TIMESTAMP) AS dt) cred
  CROSS JOIN LATERAL (SELECT CASE WHEN pmod(hash(g.id,'st'),100) < 16 AND NOT plantado.fl
                                  THEN CAST(4 AS DECIMAL(38,10))
                                  ELSE CAST(2 AS DECIMAL(38,10)) END AS status) st
""")
print(f"OK: {prest_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 SGR — Auditoria do credenciamento (1 a 3 eventos por prestador → SCD2)
# MAGIC
# MAGIC `CD_ACAO`: 1 = credenciamento inicial (1º evento), 2 = alteração cadastral.
# MAGIC A especialidade e a unidade podem mudar entre eventos; o **último** evento
# MAGIC reflete o status final (descredenciamento, se houver).
# MAGIC
# MAGIC **Defeitos propositais:**
# MAGIC - ~2% dos eventos vêm com `FL_EXCLUIDO = 1` (estorno de alteração);
# MAGIC - ~2% são **eventos vazios**: `NU_UNIDADE`, `FL_STATUS_PRESTADOR`,
# MAGIC   `CD_ESPECIALIDADE` e `DT_CREDENCIAMENTO` todos nulos (bug do sistema de origem);
# MAGIC - ~20% dos eventos intermediários têm `NU_UNIDADE` nulo → exige preenchimento
# MAGIC   para frente (`LAST_VALUE(..., true)` em janela ordenada crescente);
# MAGIC - prestadores órfãos **não** têm auditoria (ficam só na quarentena, módulo 02).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {audit_tbl} AS
WITH prestador_base AS (
  SELECT NU_PRESTADOR, CD_ESTABELECIMENTO, CD_CNES, FL_STATUS_PRESTADOR,
         DT_CREDENCIAMENTO, DT_DESCREDENCIAMENTO, CD_MOTIVO_DESCREDENCIAMENTO,
         NU_CAPACIDADE_ATEND_MES,
         -- cada prestador recebe de 1 a 3 eventos de auditoria (0..max_ev)
         pmod(hash(NU_PRESTADOR,'nev'),3) AS max_ev
  FROM {prest_tbl}
  WHERE CD_ESTABELECIMENTO <> 9999   -- órfãos não têm auditoria (só quarentena, mód. 02)
),
eventos_expandidos AS (
  -- um registro de auditoria por evento do prestador
  SELECT b.*, explode(sequence(0, b.max_ev)) AS evento
  FROM prestador_base b
),
eventos AS (
  SELECT
    e.*,
    -- ~2% dos eventos vêm completamente vazios (bug do sistema de origem)
    CASE WHEN pmod(hash(e.NU_PRESTADOR,'vazio',e.evento),50) = 0 THEN 1 ELSE 0 END AS vazio_fl
  FROM eventos_expandidos e
)
SELECT
  CASE WHEN p.evento = 0 THEN CAST(1 AS DECIMAL(38,10)) ELSE CAST(2 AS DECIMAL(38,10)) END AS CD_ACAO,
  p.NU_PRESTADOR,
  p.CD_ESTABELECIMENTO,
  p.CD_CNES,
  -- evento vazio: todos os atributos de negócio nulos
  CASE WHEN p.vazio_fl = 1 THEN NULL
       WHEN p.evento = p.max_ev THEN p.FL_STATUS_PRESTADOR
       ELSE CAST(2 AS DECIMAL(38,10)) END                                             AS FL_STATUS_PRESTADOR,
  CASE WHEN p.vazio_fl = 1 THEN NULL
       ELSE CAST(1 + pmod(hash(p.NU_PRESTADOR,'esp',p.evento),8) AS DECIMAL(38,10)) END AS CD_ESPECIALIDADE,
  -- ~20% dos eventos vêm sem unidade (além dos eventos vazios)
  CASE WHEN p.vazio_fl = 1 OR pmod(hash(p.NU_PRESTADOR,'unin',p.evento),5) = 0 THEN NULL
       ELSE CAST(1 + pmod(hash(p.NU_PRESTADOR,'uni',p.evento),4) AS DECIMAL(38,10)) END AS NU_UNIDADE,
  CASE WHEN p.vazio_fl = 1 THEN NULL
       ELSE CAST(p.DT_CREDENCIAMENTO AS TIMESTAMP) END                                AS DT_CREDENCIAMENTO,
  CASE WHEN p.evento = p.max_ev AND p.FL_STATUS_PRESTADOR = 4
       THEN p.DT_DESCREDENCIAMENTO END                                                AS DT_DESCREDENCIAMENTO,
  CASE WHEN p.evento = p.max_ev AND p.FL_STATUS_PRESTADOR = 4
       THEN p.CD_MOTIVO_DESCREDENCIAMENTO END                                         AS CD_MOTIVO_DESCREDENCIAMENTO,
  p.NU_CAPACIDADE_ATEND_MES,
  CAST(DATE_ADD(p.DT_CREDENCIAMENTO, p.evento * (180 + pmod(hash(p.NU_PRESTADOR, p.evento),1200))) AS TIMESTAMP) AS DT_AUDIT,
  CASE WHEN pmod(hash(p.NU_PRESTADOR,'op'),2)=0 THEN 'SIS_REDE' ELSE 'ANALISTA_REDE' END AS CD_OPERADOR,
  -- ~2% dos eventos são estornos (FL_EXCLUIDO = 1)
  CAST(CASE WHEN pmod(hash(p.NU_PRESTADOR,'aexc',p.evento),50)=0 THEN 1 ELSE 0 END AS DECIMAL(38,10)) AS FL_EXCLUIDO,
  CONCAT(CAST(p.NU_PRESTADOR AS STRING), '_', CAST(p.evento AS STRING))               AS merge_key,
  CURRENT_TIMESTAMP()                                                                 AS dt_carga_bronze
FROM eventos p
""")
print(f"OK: {audit_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 SGB — Planos comercializados
# MAGIC `CD_SEGMENTACAO`: 1 Ambulatorial · 2 Hospitalar · 3 Ambulatorial + Hospitalar.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {plano_tbl} AS
SELECT
  CAST(CD_PLANO AS DECIMAL(38,10))            AS CD_PLANO,
  NM_PLANO                                    AS NM_PLANO,
  CAST(CD_SEGMENTACAO AS DECIMAL(38,10))      AS CD_SEGMENTACAO,
  CAST(FL_COPARTICIPACAO AS DECIMAL(38,10))   AS FL_COPARTICIPACAO,
  NM_ACOMODACAO                               AS NM_ACOMODACAO,
  CAST(VL_MENSALIDADE_BASE AS DECIMAL(38,10)) AS VL_MENSALIDADE_BASE,
  CAST(0 AS DECIMAL(38,10))                   AS FL_EXCLUIDO,
  CURRENT_TIMESTAMP()                         AS dt_carga_bronze
FROM VALUES
  (1, 'Ambulatorial Essencial',   1, 1, 'Nao se aplica',  180.00),
  (2, 'Ambulatorial Plus',        1, 0, 'Nao se aplica',  260.00),
  (3, 'Hospitalar Enfermaria',    2, 1, 'Enfermaria',     410.00),
  (4, 'Hospitalar Apartamento',   2, 0, 'Apartamento',    680.00),
  (5, 'Completo Enfermaria',      3, 1, 'Enfermaria',     590.00),
  (6, 'Completo Apartamento',     3, 0, 'Apartamento',    980.00)
AS t(CD_PLANO, NM_PLANO, CD_SEGMENTACAO, FL_COPARTICIPACAO, NM_ACOMODACAO, VL_MENSALIDADE_BASE)
""")
print(f"OK: {plano_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 SGB — Beneficiários (carteira)
# MAGIC
# MAGIC > **Sem dados identificáveis**: nenhuma coluna de nome, CPF ou endereço.
# MAGIC > Apenas número de carteira sintético, plano, região, faixa etária e sexo.
# MAGIC
# MAGIC `NU_FAIXA_ETARIA`: 1 a 10 (faixas no padrão ANS, de 0-18 até 59+).
# MAGIC ~7% dos beneficiários estão cancelados (`FL_ATIVO = 0`).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {benef_tbl} AS
SELECT
  CAST(500000 + b.id AS DECIMAL(38,10))                                            AS NU_BENEFICIARIO,
  CONCAT('CART', LPAD(CAST(b.id AS STRING), 9, '0'))                               AS NU_CARTEIRA,
  CAST(1 + pmod(hash(b.id,'plano'),6) AS DECIMAL(38,10))                           AS CD_PLANO,
  element_at(array('SP','SP','SP','RJ','MG','PR','RS','BA','PE','GO'), b.geo)      AS SG_UF,
  element_at(array('Sudeste','Sudeste','Sudeste','Sudeste','Sudeste',
                   'Sul','Sul','Nordeste','Nordeste','Centro-Oeste'), b.geo)       AS NM_REGIAO,
  CAST(1 + pmod(hash(b.id,'faixa'),10) AS DECIMAL(38,10))                          AS NU_FAIXA_ETARIA,
  CASE WHEN pmod(hash(b.id,'sexo'),2)=0 THEN 'F' ELSE 'M' END                      AS CD_SEXO,
  CAST(TIMESTAMP(DATE_ADD(DATE'2015-01-01', pmod(hash(b.id,'ades'), 3500))) AS TIMESTAMP) AS DT_ADESAO,
  CASE WHEN b.cancel = 1
       THEN CAST(TIMESTAMP(DATE_ADD(DATE'2024-06-01', pmod(hash(b.id,'canc'), 700))) AS TIMESTAMP) END AS DT_CANCELAMENTO,
  CAST(CASE WHEN b.cancel = 1 THEN 0 ELSE 1 END AS DECIMAL(38,10))                 AS FL_ATIVO,
  CAST(CASE WHEN pmod(hash(b.id,'bexc'),200)=0 THEN 1 ELSE 0 END AS DECIMAL(38,10)) AS FL_EXCLUIDO,
  CAST(500000 + b.id AS STRING)                                                    AS merge_key,
  CURRENT_TIMESTAMP()                                                              AS dt_carga_bronze
FROM (
  SELECT id,
         1 + pmod(hash(id,'geo'),10)                                  AS geo,
         CASE WHEN pmod(hash(id,'cancel'),100) < 7 THEN 1 ELSE 0 END   AS cancel
  FROM (SELECT explode(sequence(1, 20000)) AS id)
) b
""")
print(f"OK: {benef_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 SIA — Catálogo de procedimentos
# MAGIC
# MAGIC `CD_GRUPO_PROCEDIMENTO`: 1 Consulta · 2 Exame · 3 Terapia · 4 Internação · 5 Cirurgia.
# MAGIC `VL_REFERENCIA` é o valor de tabela do procedimento (base para o custo da conta).

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {proc_tbl} AS
SELECT
  CAST(40000000 + p.id AS DECIMAL(38,10))                                              AS CD_PROCEDIMENTO,
  CONCAT(element_at(array('Consulta','Exame','Terapia','Internacao','Cirurgia'), p.grupo),
         ' - ', LPAD(CAST(p.id AS STRING), 3, '0'))                                    AS NM_PROCEDIMENTO,
  CAST(p.grupo AS DECIMAL(38,10))                                                      AS CD_GRUPO_PROCEDIMENTO,
  element_at(array('Consulta','Exame','Terapia','Internacao','Cirurgia'), p.grupo)      AS NM_GRUPO_PROCEDIMENTO,
  CAST(ROUND(element_at(array(150.00, 95.00, 240.00, 4800.00, 9500.00), p.grupo)
             * (0.70 + pmod(hash(p.id,'fator'),61)/100.0), 2) AS DECIMAL(38,10))       AS VL_REFERENCIA,
  CAST(CASE WHEN p.grupo >= 4 THEN 1 ELSE 0 END AS DECIMAL(38,10))                     AS FL_ALTA_COMPLEXIDADE,
  CAST(0 AS DECIMAL(38,10))                                                            AS FL_EXCLUIDO,
  CURRENT_TIMESTAMP()                                                                  AS dt_carga_bronze
FROM (
  SELECT id, 1 + pmod(hash(id,'grupo'),5) AS grupo
  FROM (SELECT explode(sequence(1, 200)) AS id)
) p
""")
print(f"OK: {proc_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.7 SIA — Contas médicas (guias de atendimento) — a tabela fato
# MAGIC
# MAGIC 120.000 contas em 12 competências (2025-01 a 2025-12), com **concentração**
# MAGIC em ~30 prestadores de grande porte (20% do volume), como acontece na prática.
# MAGIC
# MAGIC **Defeitos propositais (matéria-prima dos módulos 03 e 06):**
# MAGIC
# MAGIC | Defeito | Volume | Onde é tratado |
# MAGIC |---|---|---|
# MAGIC | `NU_PRESTADOR` inexistente (79999) | ~1,5% | mód. 03 — integridade referencial |
# MAGIC | `NU_PRESTADOR` nulo | ~0,4% | mód. 03 — quarentena |
# MAGIC | `CD_PROCEDIMENTO` inválido (99999999) | ~1% | mód. 03 — integridade referencial |
# MAGIC | `CD_ESTABELECIMENTO` divergente do prestador | ~0,8% | mód. 03 — regra de consistência |
# MAGIC | `DT_APRESENTACAO` anterior ao atendimento | ~0,7% | mód. 03 — regra de data |
# MAGIC | `VL_PAGO` > `VL_APRESENTADO` | ~0,5% | mód. 03 — regra de valor |
# MAGIC | `VL_PAGO` negativo | ~0,25% | mód. 03 / mód. 06 |
# MAGIC | `FL_EXCLUIDO = 1` (conta estornada) | ~1,25% | mód. 03 — filtro |
# MAGIC | `NU_AUTORIZACAO` nulo | ~6% | mód. 06 — métrica informativa |
# MAGIC | **guia duplicada** (reprocessamento) | ~2% | mód. 03 — dedup `QUALIFY` |
# MAGIC | atendimento após descredenciamento | variável | mód. 06 — métrica de atenção |
# MAGIC
# MAGIC **Anomalias de custo plantadas (respostas do desafio final):**
# MAGIC - 10 prestadores (índices 7, 23, 41, 88, 152, 246, 333, 417, 501, 588) têm
# MAGIC   valor apresentado **4x** maior a partir da competência **2025-10**;
# MAGIC - os estabelecimentos **9 (Salvador/BA)** e **10 (Recife/PE)** — região
# MAGIC   **Nordeste** — têm alta de **1,6x** no mesmo período.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {conta_tbl} AS
WITH base AS (
  SELECT
    g.id,
    -- concentração: 20% das contas caem em 30 prestadores de grande porte
    CASE WHEN pmod(hash(g.id,'skew'),100) < 20 THEN 1 + pmod(hash(g.id,'big'),30)
         ELSE 1 + pmod(hash(g.id,'prest'),600) END                       AS prest_idx,
    1 + pmod(hash(g.id,'benef'),20000)                                   AS benef_idx,
    1 + pmod(hash(g.id,'proc'),200)                                      AS proc_idx,
    DATE_ADD(DATE'2025-01-01', pmod(hash(g.id,'dia'),365))               AS dt_atend,
    1 + pmod(hash(g.id,'qt'),3)                                          AS qt_item
  FROM (SELECT explode(sequence(1, 120000)) AS id) g
),
joined AS (
  SELECT
    b.id, b.prest_idx, b.benef_idx, b.qt_item, b.dt_atend,
    p.NU_PRESTADOR                                     AS prest_nu,
    p.CD_ESTABELECIMENTO                               AS prest_estab,
    CAST(p.DT_DESCREDENCIAMENTO AS DATE)               AS prest_dt_desc,
    pr.CD_PROCEDIMENTO                                 AS proc_cd,
    pr.VL_REFERENCIA                                   AS proc_vl,
    CAST(DATE_FORMAT(b.dt_atend, 'yyyyMM') AS INT)     AS competencia
  FROM base b
  INNER JOIN {prest_tbl} p  ON p.NU_PRESTADOR    = 70000 + b.prest_idx
  INNER JOIN {proc_tbl}  pr ON pr.CD_PROCEDIMENTO = 40000000 + b.proc_idx
),
valorado AS (
  SELECT
    j.*,
    -- fator de anomalia: prestadores plantados (4x) e regiao Nordeste (1,6x) a partir de 2025-10
    CASE WHEN array_contains(array(7,23,41,88,152,246,333,417,501,588), j.prest_idx)
              AND j.competencia >= 202510 THEN 4.0
         WHEN j.prest_estab IN (9, 10) AND j.competencia >= 202510 THEN 1.6
         ELSE 1.0 END                                  AS fator_anomalia
  FROM joined j
),
apresentado AS (
  SELECT
    v.*,
    CAST(ROUND(v.proc_vl * v.qt_item
               * (0.85 + pmod(hash(v.id,'var'),40)/100.0)
               * v.fator_anomalia, 2) AS DECIMAL(38,10))  AS vl_apresentado
  FROM valorado v
),
glosado AS (
  SELECT
    a.*,
    CAST(CASE WHEN pmod(hash(a.id,'gl'),100) < 12
              THEN ROUND(a.vl_apresentado * (0.05 + pmod(hash(a.id,'glp'),30)/100.0), 2)
              ELSE 0 END AS DECIMAL(38,10))              AS vl_glosa
  FROM apresentado a
),
contas AS (
  SELECT
    CAST(9000000 + g.id AS DECIMAL(38,10))                                             AS NU_GUIA,
    CAST(500000 + g.benef_idx AS DECIMAL(38,10))                                       AS NU_BENEFICIARIO,
    -- ~0,4% prestador nulo; ~1,5% prestador inexistente
    CASE WHEN pmod(hash(g.id,'pnul'),250) = 0 THEN CAST(NULL AS DECIMAL(38,10))
         WHEN pmod(hash(g.id,'pnex'),200) < 3 THEN CAST(79999 AS DECIMAL(38,10))
         ELSE g.prest_nu END                                                            AS NU_PRESTADOR,
    -- ~0,8% estabelecimento divergente do cadastro do prestador
    CAST(CASE WHEN pmod(hash(g.id,'einc'),120) = 0 THEN 1 + pmod(hash(g.id,'e2'),12)
              ELSE g.prest_estab END AS DECIMAL(38,10))                                AS CD_ESTABELECIMENTO,
    -- ~1% codigo de procedimento invalido
    CASE WHEN pmod(hash(g.id,'pinv'),100) = 0 THEN CAST(99999999 AS DECIMAL(38,10))
         ELSE g.proc_cd END                                                            AS CD_PROCEDIMENTO,
    CAST(1 + pmod(hash(g.id,'tpat'),4) AS DECIMAL(38,10))                              AS CD_TIPO_ATENDIMENTO,
    CAST(TIMESTAMP(g.dt_atend) AS TIMESTAMP)                                           AS DT_ATENDIMENTO,
    -- ~0,7% apresentacao anterior ao atendimento (data inconsistente)
    CAST(TIMESTAMP(CASE WHEN pmod(hash(g.id,'dinv'),150) = 0
                        THEN DATE_ADD(g.dt_atend, -15)
                        ELSE DATE_ADD(g.dt_atend, 5 + pmod(hash(g.id,'apres'),40)) END) AS TIMESTAMP) AS DT_APRESENTACAO,
    CAST(g.competencia AS DECIMAL(38,10))                                              AS NU_COMPETENCIA,
    CAST(g.qt_item AS DECIMAL(38,10))                                                  AS QT_ITEM,
    g.vl_apresentado                                                                   AS VL_APRESENTADO,
    g.vl_glosa                                                                         AS VL_GLOSA,
    -- ~0,5% pago maior que apresentado; ~0,25% pago negativo
    CAST(CASE WHEN pmod(hash(g.id,'vinv'),200) = 0 THEN ROUND(g.vl_apresentado * 1.30, 2)
              WHEN pmod(hash(g.id,'vneg'),400) = 0 THEN -1 * g.vl_apresentado
              ELSE g.vl_apresentado - g.vl_glosa END AS DECIMAL(38,10))                AS VL_PAGO,
    -- ~6% sem numero de autorizacao (guia liberada sem autorizacao previa)
    CASE WHEN pmod(hash(g.id,'aut'),100) < 6 THEN CAST(NULL AS STRING)
         ELSE CONCAT('AUT', LPAD(CAST(g.id AS STRING), 9, '0')) END                    AS NU_AUTORIZACAO,
    CAST(CASE WHEN g.prest_dt_desc IS NOT NULL AND g.dt_atend > g.prest_dt_desc THEN 1
              ELSE 0 END AS DECIMAL(38,10))                                            AS FL_ATEND_POS_DESCRED,
    CAST(CASE WHEN pmod(hash(g.id,'cexc'),80) = 0 THEN 1 ELSE 0 END AS DECIMAL(38,10)) AS FL_EXCLUIDO,
    CONCAT(CAST(9000000 + g.id AS STRING), '_1')                                       AS merge_key,
    TIMESTAMP'2026-01-05 03:00:00'                                                     AS dt_carga_bronze
  FROM glosado g
)
-- Carga original
SELECT * FROM contas
UNION ALL
-- ~2% das guias foram reenviadas e reprocessadas: mesma NU_GUIA, carga mais nova
SELECT
  NU_GUIA, NU_BENEFICIARIO, NU_PRESTADOR, CD_ESTABELECIMENTO, CD_PROCEDIMENTO,
  CD_TIPO_ATENDIMENTO, DT_ATENDIMENTO,
  TIMESTAMPADD(DAY, 3, DT_APRESENTACAO)                        AS DT_APRESENTACAO,
  NU_COMPETENCIA, QT_ITEM, VL_APRESENTADO,
  CAST(ROUND(VL_APRESENTADO * 0.03, 2) AS DECIMAL(38,10))      AS VL_GLOSA,
  CAST(ROUND(VL_APRESENTADO * 0.97, 2) AS DECIMAL(38,10))      AS VL_PAGO,
  NU_AUTORIZACAO, FL_ATEND_POS_DESCRED, FL_EXCLUIDO,
  CONCAT(CAST(NU_GUIA AS STRING), '_2')                        AS merge_key,
  TIMESTAMP'2026-01-12 03:00:00'                               AS dt_carga_bronze
FROM contas
WHERE pmod(hash(NU_GUIA,'dup'),50) = 0
""")
print(f"OK: {conta_tbl}")
# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Conferência dos dados gerados

# COMMAND ----------

check = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {estab_tbl}) AS qt_estabelecimento,
        (SELECT COUNT(*) FROM {prest_tbl}) AS qt_prestador,
        (SELECT COUNT(*) FROM {audit_tbl}) AS qt_auditoria,
        (SELECT COUNT(*) FROM {plano_tbl}) AS qt_plano,
        (SELECT COUNT(*) FROM {benef_tbl}) AS qt_beneficiario,
        (SELECT COUNT(*) FROM {proc_tbl})  AS qt_procedimento,
        (SELECT COUNT(*) FROM {conta_tbl}) AS qt_conta_medica
""")
display(check)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanidade: os defeitos propositais estão lá?
# MAGIC Estes números são o "mapa do tesouro" dos exercícios de qualidade.

# COMMAND ----------

sanidade = spark.sql(f"""
    SELECT
        (SELECT COUNT(*) FROM {prest_tbl} WHERE CD_ESTABELECIMENTO = 9999)      AS prestadores_orfaos,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE NU_PRESTADOR IS NULL)           AS contas_sem_prestador,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE NU_PRESTADOR = 79999)           AS contas_prestador_inexistente,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE CD_PROCEDIMENTO = 99999999)     AS contas_procedimento_invalido,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE VL_PAGO > VL_APRESENTADO)       AS contas_pago_maior_apresentado,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE VL_PAGO < 0)                    AS contas_pago_negativo,
        (SELECT COUNT(*) - COUNT(DISTINCT NU_GUIA) FROM {conta_tbl})            AS guias_duplicadas,
        (SELECT COUNT(*) FROM {conta_tbl} WHERE FL_ATEND_POS_DESCRED = 1)       AS contas_pos_descredenciamento
""")
display(sanidade)

# COMMAND ----------

print("Setup concluído! Seus dados sintéticos estão prontos no seu schema.")
print(f"Schema de trabalho -> {target_schema}")
print("Lembre-se: todos os dados são SINTÉTICOS. Nenhum paciente ou prestador real.")
print("Próximo passo: abra o módulo 00 (Contexto de Negócio e Exploração).")
