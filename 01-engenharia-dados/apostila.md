# Workshop Amil x Databricks — Engenharia de Dados (L200)

## Apostila do Participante

Bem-vindo(a) ao workshop **hands-on** de Engenharia de Dados com Databricks!
Este material é 100% em **SQL** — você não precisa saber Python.

> **Todos os dados utilizados no workshop são sintéticos e não representam
> pacientes reais.**

---

## O problema de negócio

A Diretoria Financeira da operadora detectou que o **custo assistencial** — o
quanto a operadora paga aos prestadores pelos atendimentos dos beneficiários —
cresceu bem acima do orçado no último trimestre. Ninguém consegue explicar por quê.

As perguntas que ninguém responde hoje:

- O custo subiu porque houve **mais atendimento** ou porque o **mesmo
  atendimento ficou mais caro**?
- Existem prestadores cobrando **fora do padrão** dos seus pares?
- O aumento está concentrado em alguém específico ou é **difuso**?

Os dados existem, espalhados em três sistemas, e ninguém confia neles: contas
duplicadas, prestadores sem cadastro, valores impossíveis, histórico cadastral
que nunca foi tratado.

**Seu trabalho hoje:** construir o pipeline que torna esses dados confiáveis e
produzir a tabela que responde à pergunta da Diretoria.

---

## Objetivo do dia

Produzir a tabela **`gold_custo_utilizacao_prestador`** — uma linha por
**prestador × competência**, com custo total, utilização, percentual de glosa e um
**indicador de anomalia de custo** — e usá-la no **desafio final** para explicar o
aumento do custo assistencial.

```
LANDING (origem)  →  BRONZE (cru, fiel)  →  SILVER (limpo + histórico)  →  GOLD (negócio)
```

---

## Ambiente

| Recurso | Onde | Acesso |
|---|---|---|
| Seu espaço de trabalho | `amil_workshop_trilha_tech.<seu_usuario>` | **leitura e escrita** |

**Não há fonte central compartilhada.** No notebook de setup, **você mesmo gera**
os seus dados sintéticos dentro do seu próprio schema. Todo o pipeline acontece
isolado ali. A geração é **determinística** (usa `hash()`, não `rand()`), então
todos obtêm exatamente os mesmos dados — e dá para comparar com o gabarito.

### As 7 tabelas de landing (3 sistemas de origem)

| Sistema | Tabela | Conteúdo | Papel |
|---|---|---|---|
| **SGR** (rede credenciada) | `raw_sgr_tb_estabelecimento` | hospitais, clínicas e laboratórios da rede | dimensão |
| | `raw_sgr_tb_prestador` | prestadores credenciados (especialidade, status, capacidade) | dimensão (estado atual) |
| | `raw_sgr_au_prestador` | auditoria: histórico de alterações cadastrais | linha do tempo (SCD2) |
| **SGB** (beneficiários) | `raw_sgb_tb_plano` | planos comercializados (segmentação, acomodação) | dimensão |
| | `raw_sgb_tb_beneficiario` | beneficiários (plano, UF, faixa etária ANS) — **sem PII** | dimensão |
| **SIA** (contas médicas) | `raw_sia_tb_procedimento` | tabela de procedimentos com valor de referência | dimensão |
| | `raw_sia_tb_conta_medica` | contas médicas (guias) apresentadas e pagas | **fato** |

### Sem dado pessoal, por desenho

O beneficiário não tem nome, CPF, endereço, telefone nem qualquer informação
clínica — só identificador interno, carteira, plano, UF, região, faixa etária ANS
e sexo. Análise de custo assistencial **não precisa** de dado sensível, e essa é
uma decisão de arquitetura, não um detalhe. A LGPD aparece aqui como consciência
de contexto; o tema técnico do dia é engenharia de dados.

---

## Glossário rápido

| Termo | Significado |
|---|---|
| **Operadora** | quem vende o plano e paga o atendimento |
| **Beneficiário** | pessoa coberta pelo plano |
| **Prestador** | médico, clínica, hospital ou laboratório que atende |
| **Rede credenciada** | conjunto de prestadores contratados |
| **Estabelecimento** | unidade física (hospital, clínica, laboratório) |
| **Conta médica / guia** | documento de cobrança de um atendimento |
| **Glosa** | valor da conta recusado pela operadora |
| **Competência** | mês de referência contábil do atendimento (ex.: `202510`) |
| **Custo assistencial** | total efetivamente pago aos prestadores |
| **Utilização** | volume de atendimentos consumido |
| **Autorização** | liberação prévia do procedimento |
| **Descredenciamento** | saída do prestador da rede |
| **Faixa etária ANS** | faixas de idade padronizadas pela ANS (1 a 10) |

---

## Como você vai trabalhar: Databricks Assistant

Neste workshop você **não digita SQL na mão**. Em vez disso, descreve em português
o que quer e deixa o **Databricks Assistant** (a IA do notebook) gerar o código:

1. Numa célula vazia, abra o Assistant (ícone ✨ ou `Cmd/Ctrl + I`).
2. Cole/adapte o **PROMPT sugerido** do exercício.
3. **Sempre revise** o SQL gerado antes de rodar.

O Assistant é rápido a escrever SQL e péssimo a decidir **o que** deve ser escrito.
Descrever a transformação com precisão — grão, chave, regra, critério de
desempate — é a habilidade que o workshop treina. Quando um prompt der resultado
errado, a pergunta certa é "o que faltou na minha descrição?".

---

## Isolamento por schema: como os notebooks fazem

Cada participante tem o seu próprio schema, e todo notebook começa dizendo onde
você está:

```sql
USE CATALOG amil_workshop_trilha_tech;
USE SCHEMA seu_usuario;   -- 👈 troque pelo seu

SELECT * FROM brz_conta_medica;
```

Esse é o namespace de três níveis do Unity Catalog — `catálogo.schema.tabela`.
`USE CATALOG` e `USE SCHEMA` fixam os dois primeiros níveis na sessão, e a partir
daí você escreve só o nome da tabela. O ganho não é digitar menos: é que o mesmo
SQL do exercício funciona em qualquer schema, e trocar de ambiente (sandbox para
produção, por exemplo) é mudar duas linhas no topo, não trinta referências
espalhadas pelo código.

Duas consequências práticas. Primeira: o `USE` vale para a **sessão**, não para o
arquivo — repita as duas linhas no início de cada notebook e novamente se o
compute reiniciar. Segunda: se uma consulta reclamar que a tabela não existe,
confira antes de tudo onde você está:

```sql
SELECT current_catalog(), current_schema();
```

---

## Roteiro dos módulos

| # | Notebook | O que você aprende | ⭐ |
|---|---|---|---|
| 00 | `00_setup_exploracao` | o problema, as origens e como perfilar dados que você não conhece | 1 |
| 01 | `01_bronze_ingestao` | ingestão fiel: por que **não** se corrige nada no bronze | 1 |
| 02 | `02_silver_prestador_estabelecimento` | tipagem, deduplicação, integridade referencial, quarentena | 2 |
| 03 | `03_silver_contas_medicas` | regras de negócio na fato e quarentena **com motivo** | 2 |
| 04 | `04_silver_vigencias_credenciamento` | SCD Tipo 2, precedência `AND`/`OR`, preenchimento para frente | 2 |
| 05 | `05_gold_custo_utilizacao` | grão, as-of join, métrica de anomalia que não se engana | 1 |
| 06 | `06_qualidade_dados` | painel `dq_metricas`, severidades e porta de qualidade | 1 |
| 07 | `07_lakeflow_pipeline` | o mesmo pipeline de forma **declarativa** (sem exercício) | — |
| 08 | `08_consumo_desafio_final` | orquestração + **desafio final** | 1 + desafio |

⭐ = exercício-chave. O restante das células já vem pronto: o tempo é investido nos
pontos onde a decisão de engenharia importa.

---

## Cinco ideias que você leva para o trabalho

1. **Bronze não corrige nada.** Ele é a evidência do que a origem mandou. Perder
   isso é perder a capacidade de auditar qualquer número depois.
2. **Registro rejeitado vai para quarentena, não para o lixo.** `qua_*` com
   `motivo_quarentena` responde "quanto dinheiro está preso e por quê" — um
   `WHERE` silencioso não responde nada.
3. **Grão antes de SQL.** Uma tabela gold sem grão declarado é uma tabela que
   alguém vai somar errado.
4. **Estado atual ≠ histórico.** Ler a conta de março com o cadastro de hoje
   produz relatório que não falha e mente. É para isso que existe SCD Tipo 2.
5. **A métrica define o que você consegue ver.** Custo médio por conta e índice
   sobre tabela de referência apontam culpados diferentes — e um indicador
   normalizado pelos pares é cego a aumentos que atingem o grupo todo.

---

## Regras do jogo

- Rode os módulos **em ordem**: cada um lê as tabelas do anterior.
- **Tente antes de olhar o gabarito** (`respostas/`). O gabarito é conferência, não
  atalho — e várias respostas do checkpoint só fazem sentido depois da tentativa.
- Se um número seu divergir do esperado, não conserte por cima: descubra **qual
  camada** produziu a diferença. Esse é o exercício real.
