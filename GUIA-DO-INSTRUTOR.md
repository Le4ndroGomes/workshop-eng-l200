# Guia do Instrutor — Workshop Amil x Databricks (L200)

Duração-alvo: **4 horas**, 9 módulos, um desafio final.
Formato: hands-on, 100% SQL, exercícios resolvidos com o Databricks Assistant.

> Todos os dados utilizados no workshop são sintéticos e não representam pacientes reais.

---

## Antes do workshop

1. Catálogo `amil_workshop_trilha_tech` criado; participantes com permissão de
   `CREATE SCHEMA` e `CREATE TABLE` nele.
2. `./deploy.sh` publica os notebooks em `/Workspace/Shared/amil-workshop-trilha-tech`.
3. SQL Warehouse (ou compute serverless) ligado e aquecido. O setup leva de 3 a 6
   minutos em warehouse pequeno.
4. **Rode o setup e os gabaritos uma vez no seu schema e anote os números.** A
   função `hash()` do Spark é determinística, mas os valores exatos dependem da
   versão do runtime — os números abaixo são **ordens de grandeza para conferência**,
   não constantes. O que é rigorosamente reprodutível está na seção "Âncoras fixas".
5. Se o tempo for curto: os módulos 07 (Lakeflow declarativo) e a Parte 2 do 08
   (Job) podem ser **demonstrados** em vez de executados. Nunca corte o módulo 06
   nem o desafio final.

---

## Agenda

| # | Módulo | Min | Objetivo de aprendizagem | Contexto de negócio | Conceito Databricks | ⭐ | Saída esperada | Checkpoint |
|---|---|---|---|---|---|---|---|---|
| — | Abertura | 10 | entender o problema | custo assistencial acima do orçado | — | — | turma sabe o que vai construir | todos rodaram o setup |
| 00 | Setup e exploração | 20 | perfilar dados desconhecidos | três sistemas, nenhuma confiança | Unity Catalog, `USE CATALOG`/`USE SCHEMA`, perfilagem | 1 | volumetrias + órfãos contados | "qual é a fato?" |
| 01 | Bronze | 20 | ingestão fiel e rastreável | precisamos auditar o número depois | CTAS idempotente, metadados | 1 | 7 tabelas `brz_*` | `_dt_ingestao` vs `dt_carga_bronze` |
| 02 | Silver — dimensões | 35 | tipagem, dedup, RI, quarentena | cadastro de rede sujo | `QUALIFY ROW_NUMBER`, quarentena | 2 | `slv_prestador_estabelecimento`, `slv_beneficiario_plano`, `qua_prestador_orfao` | por que `DISTINCT` não resolve |
| 03 | Silver — fato | 35 | regras de negócio e quarentena com motivo | contas duplicadas e impossíveis | `INNER JOIN` p/ RI, `CASE` ordenado | 2 | `slv_conta_medica`, `qua_conta_invalida` | o que merece quarentena |
| 04 | Silver — vigências | 35 | SCD2 e leitura histórica | prestador muda de especialidade | `LAG`, `LAST_VALUE(...,true)`, precedência `AND`/`OR` | 2 | `slv_prestador_evento`, `slv_prestador_vigencia` | por que `ASC` e não `DESC` |
| 05 | Gold | 35 | grão, as-of join, métrica de anomalia | a tabela que o negócio pediu | janelas, `XXHASH64`, as-of join | 1 | `gold_custo_utilizacao_prestador` | por que não custo médio por conta |
| 06 | Qualidade | 15 | painel e porta de qualidade | confiança no número | `UNION ALL` de métricas, reconciliação | 1 | `dq_metricas` sem `erro` | `atencao` vs `erro` |
| 07 | Lakeflow declarativo | 20 | declarativo vs imperativo | industrializar o pipeline | MVs, `LIVE.`, `CONSTRAINT EXPECT` | — | pipeline `sdp_*` executado | o que se perde com `DROP ROW` |
| 08 | Consumo + desafio | 35 | análise de causa | resposta ao CFO | Job vs Pipeline, análise | 1 + desafio | duas causas quantificadas | achou a segunda causa? |
| — | Fechamento | 10 | consolidar | — | — | — | as cinco ideias da apostila | — |

Total: 270 min de conteúdo + folga. Em turmas grandes, some 5 min por módulo de
silver e corte o módulo 07 para demonstração.

---

## Âncoras fixas (rigorosamente reprodutíveis)

Estas não dependem de versão de runtime, porque estão codificadas explicitamente
no gerador:

- **Causa A do desafio** — 10 prestadores com sobrepreço ~4× a partir da
  competência `202510`: `NU_PRESTADOR` **70007, 70023, 70041, 70088, 70152, 70246,
  70333, 70417, 70501, 70588**. Eles estão protegidos dos defeitos de cadastro, ou
  seja, **nunca** caem em quarentena — a resposta do desafio é sempre encontrável.
- **Causa B do desafio** — estabelecimentos **9 (Salvador/BA)** e **10 (Recife/PE)**,
  região **Nordeste**, com aumento de ~60% a partir de `202510`, difuso em todos os
  prestadores. **Não** é sinalizado pelo `FL_ANOMALIA_CUSTO`, por construção.
- **Estabelecimentos com versão duplicada** na origem: ids **1, 4 e 9**.
- **Estabelecimento inexistente** referenciado pelos prestadores órfãos: **9999**.
- Competências presentes: **202501 a 202512** (atendimentos distribuídos ao longo de
  todo o ano de 2025). O corte do desafio é `<= 202509` contra `>= 202510`.

---

## Números esperados (ordens de grandeza)

| Métrica | Esperado |
|---|---|
| `raw_sgr_tb_estabelecimento` | 15 linhas (12 + 3 versões antigas) |
| `raw_sgr_tb_prestador` | 600 |
| `raw_sgr_au_prestador` | ~1.200 |
| `raw_sgb_tb_beneficiario` | 20.000 |
| `raw_sia_tb_procedimento` | 200 |
| `raw_sia_tb_conta_medica` | ~122.000 (120.000 + ~2% duplicadas) |
| Prestadores órfãos (quarentena) | ~10 a 15 |
| Prestadores válidos na silver | ~580 |
| Contas válidas após a silver | ~108.000 (≈90%) |
| Linhas na gold | ~7.000 |
| Prestadores sinalizados por anomalia | ~60 a 70 linhas (10 prestadores × 3 competências) |
| Falsos positivos de anomalia | **0** — o índice normalizado dos prestadores normais fica abaixo de ~1,25, e o limiar é 1,5 |

Se a turma obtiver números **muito** diferentes, quase sempre é uma das três
causas: não rodou o módulo anterior; esqueceu de rodar o `USE CATALOG`/`USE SCHEMA`
(ou deixou `seu_usuario` sem trocar); ou
nomeou a tabela com outro nome no exercício ⭐.

---

## Armadilhas plantadas de propósito

| Onde | Armadilha | O que ensina |
|---|---|---|
| 02 | `NU_INSCRICAO_MUNICIPAL` é 100% nula | conferir nulos antes de usar coluna |
| 02 | duas versões do mesmo estabelecimento | `DISTINCT` não deduplica; o join dobraria o custo |
| 02 | prestadores apontando para estabelecimento 9999 | integridade referencial + quarentena |
| 03 | ~2% de guias duplicadas | dedup **antes** da integridade referencial |
| 03 | `VL_PAGO` negativo e maior que o apresentado | regra de valor |
| 03 | `NU_AUTORIZACAO` nula em ~6% | **não** é quarentena: é métrica de processo |
| 04 | eventos de auditoria totalmente vazios | filtro com `OR` — e a precedência |
| 04 | atributos nulos em eventos parciais | forward fill correto (`ASC`) |
| 05 | mix de procedimentos | por que custo médio por conta engana |
| 08 | aumento regional difuso | um indicador normalizado por pares é cego a ele |

---

## Onde a turma costuma travar

- **Módulo 02, ⭐1 (quarentena):** muita gente tenta achar o órfão com `INNER JOIN`.
  Dica a dar: "o que você quer são as linhas que **não** casaram".
- **Módulo 03, ⭐2 (motivo):** a ordem do `CASE` importa; uma conta pode violar
  duas regras. Pergunte qual motivo o negócio precisa ver primeiro.
- **Módulo 04, ⭐2:** se a Conferência 3 mostrar "1 especialidade" para todos, o
  preenchimento foi feito para trás. É o erro mais instrutivo do workshop — vale
  parar 5 minutos nele.
- **Módulo 05, ⭐1:** se a gold tiver menos linhas que `vw_conta_agregada`, há
  competência sem vigência. Costuma ser `<=` em vez de `<` no fim da vigência, ou
  o filtro de duração positiva ausente no módulo 04.
- **Desafio final:** a maioria acha a causa A e para. A pergunta que destrava:
  *"que tipo de aumento este indicador nunca pegaria?"*

---

## Fechamento

Peça a duas ou três pessoas que leiam em voz alta o parágrafo de resposta ao CFO.
A comparação entre as versões mostra melhor do que qualquer slide que o valor do
pipeline não está no SQL — está na decisão de qual número merece confiança.

Encerre retomando as cinco ideias da apostila e, se houver interesse, aponte os
próximos passos naturais: Unity Catalog para governança de verdade, Lakeflow
Connect para ingestão gerenciada e feature store para levar a gold ao ML. Nada
disso é necessário para o L200 — mencione como caminho, não como conteúdo.
