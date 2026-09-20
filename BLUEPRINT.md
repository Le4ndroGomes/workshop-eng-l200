# Blueprint — Adaptação do Workshop L200 para Operadora de Saúde (Amil)

Especificação completa do desenho do workshop, do modelo de dados, do mapeamento
exercício-a-exercício e do plano de implementação. Documento de referência para
arquitetos e instrutores; o participante não precisa lê-lo.

> Todos os dados utilizados no workshop são sintéticos e não representam pacientes reais.

---

## 1. Resumo executivo

O workshop original ensinava engenharia de dados em arquitetura medalhão usando
manutenção preditiva de fornos de alumínio (CBA, sistema de telemetria Gorila).
Esta adaptação mantém **integralmente** a estrutura pedagógica e os conceitos de
engenharia, e substitui o domínio por **gestão de rede e custo assistencial de uma
operadora de saúde**.

A adaptação **não** é substituição de termos. Cada exercício foi reduzido ao seu
conceito de engenharia subjacente (deduplicação por chave, integridade
referencial, SCD Tipo 2, as-of join, grão de agregação, porta de qualidade) e
reconstruído em torno de um problema de saúde que exige **o mesmo** conceito por
razões de negócio próprias. Em três casos o exercício ficou **melhor** que o
original, porque o domínio de saúde oferece motivação mais forte:

- a **quarentena** deixou de ser um descarte silencioso e passou a responder
  "quanto dinheiro está retido e por quê" — em saúde, o registro rejeitado é
  dinheiro pago;
- o **SCD Tipo 2** passou a ter consequência financeira direta (ler a conta de
  março com a especialidade de hoje distorce o custo médio de duas especialidades);
- a **métrica da gold** passou a ser objeto de ensino: o caminho óbvio (custo médio
  por conta) leva à conclusão errada, e o workshop mostra por quê.

Escolhas de escopo confirmadas com o solicitante: entrega de **blueprint +
implementação completa**; marca **Amil** com catálogo novo (`amil_workshop_trilha_tech`),
sem herança do nome anterior; **~4 horas** preservando todos os módulos com
timeboxes mais justos, mais um desafio final.

---

## 2. Análise do workshop original

**Arquitetura.** Landing (`raw_gorila_*`, geradas por script Python
determinístico) → bronze (CTAS fiel) → silver (dimensão conformada + SCD2) → gold
(`gold_forno_enriquecido`, grão forno × vigência, com `FL_FALHA`) → qualidade →
Lakeflow declarativo → consumo e orquestração.

**Pontos fortes que foram preservados.**

- Isolamento por participante com variável de sessão + `IDENTIFIER()`.
- Geração determinística com `hash()`/`pmod()`, nunca `rand()`.
- Poucos exercícios (⭐) nos pontos de decisão; o resto pronto.
- Databricks Assistant como acelerador, com prompts em português.
- Defeitos de qualidade plantados de propósito na origem.
- Par exercício/gabarito arquivo a arquivo.

**Fragilidades identificadas e corrigidas nesta adaptação.**

| Fragilidade no original | Correção |
|---|---|
| `FIRST_VALUE(col, true)` com `ORDER BY DT_FIM_VIGENCIA DESC` numa janela `UNBOUNDED PRECEDING → CURRENT ROW`: atribui o valor **mais recente** a todas as linhas, anulando o histórico e vazando informação do futuro | `LAST_VALUE(col, true)` com `ORDER BY ... ASC`, com explicação do porquê e uma conferência que detecta a versão errada |
| Rótulo-alvo (`FL_FALHA`) derivado do status atual, próximo de vazamento conceitual | o alvo virou uma **métrica de anomalia calculada**, comparável entre pares e com piso de volume |
| Registros sem integridade referencial descartados por `WHERE` | quarentena explícita `qua_*` com `motivo_quarentena` e valor retido |
| Módulo de qualidade desconectado do restante | painel `dq_metricas` com severidades e **reconciliação silver↔gold** |
| Ausência de desafio final | desafio com duas causas de naturezas diferentes |

---

## 3. Cenário de negócio proposto

**Opção escolhida: C (claims analytics) enriquecida com A (detecção de anomalia de
custo/utilização).**

Por quê, em relação às alternativas:

- **A pura (anomalia de custo/utilização)** dá um problema analítico bonito, mas
  fraco em engenharia: poucas tabelas, poucos joins, pouca necessidade de
  historicização. Não sustentaria 4 horas de pipeline.
- **B (otimização de rede)** exige geografia, adequação de rede e regras da ANS
  para ser convincente; empurra o workshop para governança e domínio regulatório —
  exatamente o que o escopo L200 pede para evitar.
- **C (claims analytics)** é o coração operacional de uma operadora: conta médica é
  uma **fato** natural, com múltiplas dimensões, defeitos realistas abundantes,
  necessidade legítima de histórico cadastral e uma pergunta de negócio que a
  diretoria realmente faz. Ao acrescentar a detecção de anomalia da opção A, o
  workshop ganha um objetivo narrativo (**por que o custo subiu?**) que amarra os
  nove módulos numa única investigação.

**A narrativa, módulo a módulo.**

| Momento | História |
|---|---|
| Abertura | O custo assistencial fechou o trimestre acima do orçado. Ninguém sabe explicar. |
| Bronze | Os dados existem em três sistemas e ninguém confia neles. Primeiro passo: registrar fielmente o que a origem manda — sem consertar nada, porque depois será preciso provar de onde veio cada número. |
| Silver | Agora, conserto com critério: o cadastro da rede tem duplicatas e prestadores fantasmas; as contas têm guias repetidas e valores impossíveis. O que não passa vai para quarentena, com motivo e valor — não para o lixo. |
| Silver (vigências) | Descoberta incômoda: prestador muda de especialidade e status ao longo do tempo. Ler a conta de março com o cadastro de hoje produziria um relatório que não falha e mente. |
| Gold | A tabela que o negócio pediu. E a primeira lição desconfortável: a métrica óbvia aponta os culpados errados. |
| Qualidade | Antes de levar número para a diretoria: ele é confiável? A gold perdeu dinheiro pelo caminho? |
| Declarativo | O mesmo pipeline, industrializado — e o que se ganha e se perde ao delegar as regras à plataforma. |
| Final | A resposta ao CFO: duas causas, de naturezas diferentes, cada uma com valor em reais e uma ação. |

---

## 4. Modelo de dados

### 4.1 Tabelas de origem

Sete tabelas em três sistemas simulados, o que força o participante a lidar com
convenções diferentes na mesma frase SQL.

| # | Tabela | Sistema | Descrição de negócio | PK | Atributos principais | Papel |
|---|---|---|---|---|---|---|
| 1 | `raw_sgr_tb_estabelecimento` | SGR | unidades físicas da rede (hospital, clínica, laboratório) | `CD_ESTABELECIMENTO` | nome, tipo, UF, município, região, leitos, CNPJ, início de operação | dimensão geográfica; fonte do corte regional |
| 2 | `raw_sgr_tb_prestador` | SGR | prestadores credenciados, estado atual | `NU_PRESTADOR` | estabelecimento, CNES, especialidade, tipo, status, capacidade mensal, datas de credenciamento e descredenciamento | dimensão principal |
| 3 | `raw_sgr_au_prestador` | SGR | auditoria de alterações cadastrais do prestador | `CD_ACAO` | mesmos atributos + `DT_AUDIT`, `CD_OPERADOR` | linha do tempo para SCD2 |
| 4 | `raw_sgb_tb_plano` | SGB | planos comercializados | `CD_PLANO` | segmentação ANS, coparticipação, acomodação, mensalidade base | dimensão |
| 5 | `raw_sgb_tb_beneficiario` | SGB | beneficiários, **sem PII** | `NU_BENEFICIARIO` | carteira, plano, UF, região, faixa etária ANS, sexo, adesão, cancelamento | dimensão |
| 6 | `raw_sia_tb_procedimento` | SIA | tabela de procedimentos e valor de referência | `CD_PROCEDIMENTO` | grupo, valor de referência, alta complexidade | dimensão; base da métrica insensível ao mix |
| 7 | `raw_sia_tb_conta_medica` | SIA | contas médicas (guias) apresentadas e pagas | `NU_GUIA` | beneficiário, prestador, estabelecimento, procedimento, tipo de atendimento, datas, competência, quantidade, valores apresentado/glosa/pago, autorização | **fato** |

### 4.2 Diagrama ER

```
  raw_sgb_tb_plano                    raw_sgr_tb_estabelecimento
        │ 1                                     │ 1
        │                                       │
        │ N                                     │ N
  raw_sgb_tb_beneficiario              raw_sgr_tb_prestador ──1───N── raw_sgr_au_prestador
        │ 1                                     │ 1
        │                                       │
        └──────────── N ─────┐     ┌───── N ─────┘
                             ▼     ▼
                     raw_sia_tb_conta_medica
                             ▲ N
                             │
                             │ 1
                     raw_sia_tb_procedimento
```

### 4.3 Defeitos de qualidade plantados

| Defeito | Onde | Volume aprox. | Conceito exercitado |
|---|---|---|---|
| prestador apontando para estabelecimento inexistente (9999) | prestador | ~2% | integridade referencial, quarentena |
| versões antigas do mesmo estabelecimento | estabelecimento | 3 ids | deduplicação por chave (e por que `DISTINCT` falha) |
| registros logicamente excluídos (`FL_EXCLUIDO = 1`) | várias | ~1–2% | exclusão lógica vs física |
| guias duplicadas | conta médica | ~2% | dedup com critério de desempate |
| `CD_PROCEDIMENTO` inexistente | conta médica | ~1% | integridade referencial |
| `DT_APRESENTACAO` anterior a `DT_ATENDIMENTO` | conta médica | ~0,7% | regra de data |
| `VL_PAGO` negativo / maior que apresentado | conta médica | ~0,8% | regra de valor |
| `NU_PRESTADOR` nulo | conta médica | ~0,4% | nulo em chave estrangeira |
| estabelecimento da conta divergente do estabelecimento do prestador | conta médica | ~0,8% | consistência entre dimensões |
| `NU_AUTORIZACAO` nula | conta médica | ~6% | o que **não** deve ir para quarentena |
| atendimento após o descredenciamento do prestador | conta médica | ~1% | achado de negócio, não erro de dado |
| eventos de auditoria totalmente vazios | auditoria | ~1,5% | filtro com `OR` e precedência de operadores |
| eventos de auditoria estornados | auditoria | ~2% | exclusão lógica no histórico |
| `NU_UNIDADE` nulo em eventos parciais | auditoria | ~20% | forward fill em SCD2 |
| coluna 100% nula (`NU_INSCRICAO_MUNICIPAL`) | estabelecimento | 100% | perfilar antes de usar |

### 4.4 Camadas construídas

**Bronze** (7 tabelas `brz_*`): cópia fiel, tipos da origem preservados, mais
`_dt_ingestao`. A distinção entre `_dt_ingestao` (quando **nós** ingerimos) e
`dt_carga_bronze` (quando a **origem** gravou) é ponto de ensino explícito —
misturar as duas é o erro que inviabiliza auditoria posterior.

**Silver** (5 tabelas `slv_*` + 2 `qua_*`):

| Tabela | Grão | Conteúdo |
|---|---|---|
| `slv_prestador_estabelecimento` | prestador | dimensão conformada, estado atual, com nome de especialidade |
| `slv_beneficiario_plano` | beneficiário | dimensão conformada, sem PII |
| `slv_conta_medica` | guia | fato limpa, com valor de referência e grupo do procedimento |
| `slv_prestador_evento` | evento | histórico da auditoria + estado atual unificados |
| `slv_prestador_vigencia` | prestador × vigência | SCD Tipo 2, intervalos `[início, fim)` |
| `qua_prestador_orfao` | prestador | rejeitados com motivo |
| `qua_conta_invalida` | guia | rejeitados com motivo e `VL_PAGO` retido |

**Gold** (1 tabela): `gold_custo_utilizacao_prestador`, grão **prestador ×
competência**. Contém chave surrogada, identificação e dimensões do prestador
**na competência** (as-of join), volumetria, custo, glosa, utilização da capacidade
e os indicadores de anomalia. Deliberadamente **não** contém plano nem faixa
etária: eles não pertencem a este grão e a análise por plano se faz na silver.

**Métrica central.**

```
IDX_CUSTO_TABELA   = SUM(VL_PAGO) / SUM(VL_REFERENCIA * QT_ITEM)
IDX_CUSTO_VS_PARES = IDX_CUSTO_TABELA / AVG(IDX_CUSTO_TABELA)
                       OVER (PARTITION BY CD_ESPECIALIDADE, NU_COMPETENCIA)
FL_ANOMALIA_CUSTO  = 1 quando IDX_CUSTO_VS_PARES >= 1.5 e QT_CONTAS >= 10
```

O índice sobre tabela de referência neutraliza o **mix de procedimentos**; a
normalização pelos pares responde "caro em relação a quem faz o mesmo tipo de
coisa"; o piso de volume evita falso positivo em prestador com poucas contas.
Validado por simulação: índice normalizado dos prestadores normais com máximo
≈1,22 contra mínimo ≈2,96 dos prestadores com sobrepreço — zero falso positivo com
limiar 1,5.

---

## 5. Mapeamento original → saúde

Nenhum arquivo foi renomeado por conveniência: cada um foi classificado, e o nome
novo reflete o conteúdo novo.

| Exercício original | Conceito de engenharia subjacente | Equivalente em saúde | Decisão |
|---|---|---|---|
| Explorar `raw_gorila_*`, contar volumes | perfilagem de fonte desconhecida | explorar 7 tabelas em 3 sistemas, contar volumes e medir o sintoma (custo por competência) | **Modificar** |
| Detectar fornos órfãos de planta | integridade referencial via `LEFT JOIN` | detectar prestadores órfãos, contas sem prestador/procedimento/beneficiário | **Modificar** (ampliado a 3 checagens) |
| CTAS das 3 tabelas bronze | ingestão fiel e idempotente | CTAS das 7 tabelas bronze (3 prontas, 4 no exercício) | **Modificar** |
| Silver forno + planta: tipagem, exclusão, dedup, join | dimensão conformada em 4 camadas de qualidade | `slv_prestador_estabelecimento` com as mesmas 4 camadas | **Modificar** |
| — (não existia) | segunda dimensão pelo mesmo padrão, para consolidar | `slv_beneficiario_plano` — e a lição "nem toda origem tem defeito" | **Criar** |
| Descarte de fornos órfãos no `WHERE` | destino de registro rejeitado | `qua_prestador_orfao` com motivo | **Substituir** (era descarte silencioso) |
| — (não existia: não havia fato transacional) | regras de negócio e quarentena na fato | `slv_conta_medica` + `qua_conta_invalida` com `motivo_quarentena` ordenado | **Criar** |
| União auditoria + estado atual, com filtro `AND`/`OR` | precedência de operadores e linha do tempo | `slv_prestador_evento`, com demonstração lado a lado via `COUNT_IF` | **Modificar** |
| Vigências com `LAG` + `FIRST_VALUE(...,DESC)` | SCD Tipo 2 e forward fill | `slv_prestador_vigencia` com `LAST_VALUE(...,ASC)` + conferência que detecta o erro | **Substituir** (o original preenchia para trás) |
| Gold forno enriquecido, grão forno × vigência, `FL_FALHA` | grão, surrogate key, junção com dimensão histórica | `gold_custo_utilizacao_prestador`, grão prestador × competência, **as-of join** e `FL_ANOMALIA_CUSTO` | **Substituir** (o rótulo virou métrica calculada; entrou as-of join) |
| Painel de qualidade | observabilidade do pipeline | `dq_metricas` com severidades, porta de qualidade e **reconciliação silver↔gold** | **Modificar** (ampliado) |
| Lakeflow declarativo com `EXPECT` | declarativo vs imperativo | 6 MVs `sdp_*` equivalentes, com quadro comparativo e discussão sobre `DROP ROW` vs quarentena | **Modificar** |
| Consumo e orquestração (Job vs Pipeline) | industrialização | mesmo conteúdo + consultas de consumo por região/especialidade/plano | **Modificar** |
| — (não existia) | síntese e análise de causa | **desafio final** com duas causas de naturezas diferentes | **Criar** |

---

## 6. Classificação dos arquivos

| Arquivo | Decisão |
|---|---|
| `00-setup/setup_participantes.py` | **Substituir** conteúdo (mesmo nome: mesmo papel) |
| `01-engenharia-dados/notebooks/00_setup_exploracao.sql` | **Modificar** |
| `01-engenharia-dados/notebooks/01_bronze_ingestao.sql` | **Modificar** |
| `…/02_silver_forno_planta.sql` | **Excluir** → criado `02_silver_prestador_estabelecimento.sql` |
| `…/03_silver_eventos_vigencias.sql` | **Excluir** → criados `03_silver_contas_medicas.sql` e `04_silver_vigencias_credenciamento.sql` |
| `…/04_gold_forno_enriquecido.sql` | **Excluir** → criado `05_gold_custo_utilizacao.sql` |
| `…/05_qualidade_dados.sql` | **Excluir** → criado `06_qualidade_dados.sql` |
| `…/06_lakeflow_pipeline.sql` | **Excluir** → criado `07_lakeflow_pipeline.sql` |
| `…/07_consumo_orquestracao.sql` | **Excluir** → criado `08_consumo_desafio_final.sql` |
| espelhos em `respostas/` | idem (sem gabarito para o módulo 07, que não tem exercício) |
| `README.md`, `01-engenharia-dados/apostila.md`, `assets/arquitetura.md` | **Substituir** conteúdo |
| `databricks.yml`, `deploy.sh` | **Modificar** (nome do bundle, destino, tarefas do Job) |
| `.databricks-resources.json` | **Excluir** (estado local referente ao catálogo antigo) |
| `00-setup/__pycache__/` | **Excluir** (artefato de build) |
| `GUIA-DO-INSTRUTOR.md`, `BLUEPRINT.md` | **Criar** |

A renumeração de 8 para 9 módulos não é cosmética: o módulo original 03 acumulava
fato e SCD2, que em saúde são dois conceitos com peso próprio (regras de negócio na
fato; historicização da dimensão). Separá-los foi o que permitiu manter timeboxes
de 35 minutos sem cortar conteúdo.

---

## 7. Estratégia de geração de dados sintéticos

**Princípios.** Determinística (`hash()` + `pmod()`, nunca `rand()`);
autocontida (nenhuma fonte externa, nenhum arquivo); isolada por participante;
100% sintética, sem PII nem dado clínico; volume calibrado por **tempo de
execução**, não por realismo.

**Técnicas.** `explode(sequence(1, N))` para gerar linhas; `pmod(hash(id, 'sal'), k)`
para atributos categóricos, com um "sal" textual diferente por atributo para
evitar correlação acidental; `element_at(array(...), idx)` e `VALUES ... AS t(...)`
para tabelas de domínio; `CROSS JOIN LATERAL` para reutilizar valores derivados
dentro da mesma linha; CTEs sequenciais (`base → joined → valorado → apresentado →
glosado`) onde uma expressão depende da anterior; `UNION ALL` de um subconjunto
filtrado para plantar duplicatas — nunca `INSERT` lendo a própria tabela destino.

**Volumes.** 12 estabelecimentos (+3 versões antigas), 600 prestadores, ~1.200
eventos de auditoria, 6 planos, 20.000 beneficiários, 200 procedimentos, 120.000
contas (+~2% duplicadas). Resultado: ~7.000 linhas na gold, com mediana de ~13
contas por prestador-competência — suficiente para que médias e janelas sejam
estatisticamente estáveis, e pequeno o bastante para rodar em minutos.

**Sinais plantados.** Um `fator_anomalia` multiplica o valor pago: 4,0 para os 10
prestadores da causa A a partir de `202510`; 1,6 para os estabelecimentos 9 e 10
(Nordeste) no mesmo período; 1,0 caso contrário. Os 10 prestadores da causa A são
**protegidos** dos sorteios de órfão, exclusão lógica e descredenciamento — sem
isso, havia risco de um deles ser filtrado na silver e a resposta do desafio variar.

**Limite conhecido.** O `hash()` do Spark é Murmur3; os volumes exatos dos defeitos
dependem do runtime. O guia do instrutor apresenta faixas e pede que os números
reais sejam anotados numa execução prévia. O que é rigorosamente reprodutível são
as âncoras codificadas explicitamente (ids dos prestadores da causa A,
estabelecimentos da causa B, ids com versão duplicada, competências 202501–202512).

---

## 8. Estratégia de qualidade de dados

Quatro camadas, sempre na mesma ordem, e a ordem é ensinada como regra:

1. **Tipagem** — a origem entrega tudo como `DECIMAL(38,10)`/`STRING`.
2. **Exclusão lógica** — `FL_EXCLUIDO = 0`.
3. **Deduplicação** — `QUALIFY ROW_NUMBER() OVER (PARTITION BY chave ORDER BY dt_carga_bronze DESC) = 1`.
4. **Integridade referencial e regras de negócio** — `INNER JOIN` + condições.

Dedup **antes** de integridade referencial: uma guia duplicada cujas versões
divergem em validade poderia, na ordem inversa, sobreviver na fato **e** na
quarentena.

**Três destinos possíveis para um registro problemático**, e a distinção é o
conteúdo do módulo 06:

| Situação | Destino | Exemplo |
|---|---|---|
| tornaria a soma errada | quarentena `qua_*`, com motivo e valor | prestador inexistente, valor pago negativo |
| dado correto, problema real de operação | permanece na fato, sinalizado, e sobe como `atencao` | conta sem autorização, atendimento pós-descredenciamento |
| quebra estrutural do modelo | severidade `erro`, **para o pipeline** | chave surrogada duplicada, custo negativo na gold, divergência na reconciliação |

A **reconciliação silver↔gold** (soma de `VL_PAGO` contra soma de
`VL_CUSTO_TOTAL`) é a métrica mais importante do painel: prova que a agregação não
perdeu nem inventou dinheiro. O gabarito discute explicitamente o que ela **não**
prova (distribuição correta entre prestadores), para que ninguém a trate como
teste suficiente.

---

## 9. Desafio final

**Problema.** A Diretoria Financeira pede a explicação do aumento de custo do
trimestre: onde aumentou, quanto de cada causa, o que fazer na semana seguinte.

**Tabelas disponíveis.** As que o participante construiu — gold, silver, quarentenas
e o painel de qualidade.

**Resultado esperado.** Duas causas de naturezas diferentes:

- **A — sobrepreço concentrado:** 10 prestadores passam a cobrar ~4× o valor de
  referência a partir de `202510`. Detectável pelo `FL_ANOMALIA_CUSTO`.
- **B — inflação regional difusa:** todos os prestadores de dois estabelecimentos
  do Nordeste sobem ~60% no mesmo período. **Invisível** ao indicador, porque ele
  normaliza pelos pares da especialidade e o grupo inteiro subiu junto.

Mais um achado operacional de brinde: contas pagas a prestadores já
descredenciados na data do atendimento — só detectável porque a dimensão é
historicizada.

**Critérios de sucesso.** Variação decomposta em volume e preço; duas causas com
valor em reais; pelo menos uma não encontrável pelo indicador; uso de métrica
insensível ao mix; painel de qualidade consultado antes de concluir; uma
recomendação acionável por causa.

**Dicas graduadas** (4 níveis, em `<details>`) e **solução do instrutor** completa,
com decomposição volume × preço, quantificação por prestador e por região
(excluindo os prestadores da causa A para não contar o mesmo dinheiro duas vezes) e
um parágrafo-modelo de resposta ao CFO.

A lição final é deliberadamente uma lição sobre limites: **nenhum indicador único
cobre todo tipo de anomalia; a dimensão de análise faz parte da detecção.**

---

## 10. Nível de complexidade (L200)

Mantido dentro do escopo. **Fora**: MLOps, ML avançado, streaming complexo,
arquiteturas de agentes, Unity Catalog além do namespace de três níveis, FHIR,
HL7, adjudicação real de sinistros, modelos atuariais, fluxos regulatórios da ANS.
**Dentro**: SQL de transformação, janelas, SCD2, as-of join, qualidade declarativa
e imperativa, orquestração básica, uma métrica analítica simples e bem justificada.

A LGPD entra como **consciência de contexto**: o modelo não tem PII nem dado
clínico, e o workshop explicita a declaração de dados sintéticos — sem se
transformar em workshop de governança.

---

## 11. Verificação

**Consistência técnica.** Toda tabela lida por um módulo é criada por um módulo
anterior; todos os nomes de coluna referenciados existem no contrato produzido pelo
gerador; os exercícios ⭐ informam explicitamente o nome da tabela a criar, porque
os módulos seguintes dependem dele; nenhuma coluna derivada apenas no gabarito é
usada por um módulo posterior; geração determinística sem `rand()`; execução
sequencial possível do módulo 00 ao 08; cada exercício tem gabarito correspondente
(exceto o módulo 07, que não tem exercício).

**Varredura de resíduo do domínio original.** Termos buscados: `forno`, `furnace`,
`Gorila`, `planta`, `telemetria`, `temperatura`, `falha`, `manutenção`, `alumínio`,
`CBA`. Classificação de cada ocorrência:

| Ocorrência | Classificação |
|---|---|
| arquivos `*_forno_*`, `*_eventos_vigencias`, `05_qualidade_dados`, `06_lakeflow_pipeline`, `07_consumo_orquestracao` (notebooks e respostas) | **Devia ser removido** → excluídos |
| `.databricks-resources.json` com schema `cba_workshop_trilha_tech.*` | **Devia ser removido** → excluído |
| `README.md`, `apostila.md`, `assets/arquitetura.md`, `databricks.yml`, `deploy.sh` | **Devia ser removido** → conteúdo substituído |
| `01-engenharia-dados/respostas/*` gerados nesta adaptação | nenhuma ocorrência |
| histórico do Git | **Não aplicável** — a cópia de trabalho veio de um ZIP do GitHub, sem `.git`. O repositório foi inicializado nesta adaptação, então o histórico começa já no domínio de saúde. Se a adaptação for levada para o repositório original como branch, o histórico do domínio industrial permanece lá como registro de versionamento e é invisível ao participante |
| este BLUEPRINT | **Intencional** — a análise do original exige nomeá-lo |

**Privacidade.** Nenhum dado real de saúde ou de paciente é utilizado; nenhuma
coluna de nome, CPF, endereço, telefone, diagnóstico, CID ou resultado de exame
existe no modelo; a declaração de dados sintéticos aparece no README, na apostila,
no diagrama de arquitetura, no guia do instrutor e no notebook 00.

**Limites da verificação feita.** O SQL foi revisado por leitura e por consistência
de contrato de colunas, e a aritmética do gerador foi reproduzida em simulação
independente; **não** houve execução em um workspace Databricks real, por ausência
de ambiente. Antes da primeira turma, o instrutor deve executar setup + módulos
00→08 uma vez e anotar os volumes efetivos (ver guia do instrutor).

---

## 12. Plano de implementação

| Fase | Conteúdo | Estado |
|---|---|---|
| 1 | Inspeção completa do repositório original e extração dos conceitos de engenharia por exercício | concluída |
| 2 | Escolha do problema de negócio, modelo de sete tabelas, defeitos plantados, sinais do desafio | concluída |
| 3 | Reescrita do gerador sintético (`setup_participantes.py`) e validação por simulação | concluída |
| 4 | Módulos 00–05: notebook + gabarito de cada | concluída |
| 5 | Módulos 06–08: qualidade, Lakeflow declarativo, consumo e desafio final | concluída |
| 6 | Arquivos de apoio: README, apostila, arquitetura, bundle, deploy, guia do instrutor, blueprint; exclusão dos arquivos do domínio antigo | concluída |
| 7 | Execução ponta a ponta em workspace real, registro dos volumes efetivos no guia do instrutor e ajuste fino de timeboxes após a primeira turma | **pendente — requer ambiente Databricks** |

---

## Apêndice — porta de qualidade do desenho

| Pergunta | Resposta |
|---|---|
| O problema de negócio é realista para uma operadora? | Sim — custo assistencial por prestador é o relatório mais pedido pela área financeira de uma operadora. |
| Um executivo entenderia o valor em uma frase? | Sim: "descobrir quanto do aumento de custo é preço e quanto é volume, e de quem". |
| A tabela gold é diretamente utilizável? | Sim — grão declarado, uma linha por prestador × competência, e o módulo 08 a consome sem nenhuma transformação adicional. |
| Cada conceito de engenharia do original tem equivalente motivado? | Sim, e três ficaram mais fortes (quarentena, SCD2, métrica). Ver matriz da seção 5. |
| A progressão de dificuldade é crescente? | Sim: perfilar → copiar → limpar dimensão → limpar fato → historicizar → agregar → medir → industrializar → investigar. |
| Os exercícios são poucos e significativos? | 13 exercícios ⭐ em 9 módulos, todos em pontos de decisão; o restante vem pronto. |
| A terminologia é a que se usa no mercado brasileiro? | Sim, com glossário de 13 termos na apostila para quem vem de outro domínio. |
| Algum dado real de paciente é usado? | Não. Nenhum PII, nenhum dado clínico; declaração de dados sintéticos em cinco arquivos. |
| Vira workshop de governança? | Não — LGPD aparece como consequência do desenho, em dois parágrafos, sem exercício. |
| Cabe em 4 horas? | Sim, com 270 min de conteúdo e orientação explícita sobre o que demonstrar em vez de executar. |
| Roda de forma reprodutível para todos? | Sim — geração determinística e âncoras codificadas; a única variação possível é de volumetria por versão de runtime, e o guia trata isso como faixa. |
| Um arquiteto consegue usar este documento como especificação? | Sim: modelo de dados, métricas, defeitos plantados, sinais do desafio e classificação arquivo a arquivo estão todos aqui. |

Nenhuma resposta ficou negativa. O único item em aberto não é de desenho: é a
execução de validação em workspace real (fase 7).
