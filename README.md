# Workshop Amil x Databricks — Engenharia de Dados (L200)

Workshop **hands-on**, 100% **SQL**, de engenharia de dados em arquitetura
medalhão, com caso de uso de **gestão de rede e custo assistencial** de uma
operadora de saúde: descobrir **por que o custo assistencial aumentou** e **onde**.

> **Todos os dados utilizados no workshop são sintéticos e não representam
> pacientes reais.**

## Objetivo

Construir, camada a camada (landing → bronze → silver → gold), a tabela
`gold_custo_utilizacao_prestador` — custo e utilização por **prestador ×
competência**, com indicador de anomalia de custo — e usá-la para responder a uma
pergunta real da Diretoria Financeira no desafio final.

## Público-alvo

Times que trabalham com **SQL** (não é necessário Python). O único código Python é
o notebook de setup, rodado uma vez.

Os exercícios são resolvidos com o **Databricks Assistant**: o participante
escreve um prompt em português e a IA gera o SQL. Cada notebook tem poucos
exercícios (⭐), sempre nos pontos-chave; o restante já vem em células prontas.

## Estrutura do repositório

```
amil-workshop-trilha-tech/
├── 00-setup/
│   └── setup_participantes.py       # cada participante roda 1x: cria seu schema
│                                    # E gera seus próprios dados sintéticos
├── 01-engenharia-dados/
│   ├── apostila.md                  # guia do participante (leia primeiro)
│   ├── notebooks/                   # exercícios em branco (00 a 08)
│   └── respostas/                   # gabarito validado
├── assets/arquitetura.md            # diagrama da arquitetura
├── GUIA-DO-INSTRUTOR.md             # agenda, checkpoints, números esperados
├── BLUEPRINT.md                     # especificação do desenho do workshop
├── databricks.yml                   # bundle (opcional) para deploy dos notebooks
├── deploy.sh                        # script de deploy p/ o workspace do instrutor
└── README.md
```

## Ambiente

| Recurso | Local | Acesso participante |
|---|---|---|
| Trabalho (tudo) | `amil_workshop_trilha_tech.<usuario>` | leitura/escrita |

Não há fonte central: cada participante **gera os próprios dados** no setup, dentro
do seu schema. Todo o pipeline fica isolado ali.

## Passo a passo

### Instrutor (uma vez, antes do workshop)

1. **Publicar os notebooks** no workspace: `./deploy.sh` (usa a CLI do Databricks).
2. Garantir que o catálogo `amil_workshop_trilha_tech` **já existe** e que os
   participantes têm permissão de **criar schema e tabelas** nele.
3. Rodar o setup e os gabaritos uma vez no próprio schema e **anotar os números**
   (ver `GUIA-DO-INSTRUTOR.md`).

### Participante

1. Rodar `00-setup/setup_participantes.py`: cria
   `amil_workshop_trilha_tech.<seu_usuario>` **e gera** as tabelas de origem
   (`raw_sgr_*`, `raw_sgb_*`, `raw_sia_*`) — 12 estabelecimentos, 600 prestadores,
   ~1.200 eventos de auditoria cadastral, 6 planos, 20.000 beneficiários,
   200 procedimentos e ~122.000 contas médicas, com defeitos de qualidade
   intencionais.
2. Abrir a `apostila.md` e seguir os módulos 00 → 08 usando
   `01-engenharia-dados/notebooks/`.
3. Conferir com o gabarito em `01-engenharia-dados/respostas/` **após** tentar.

## Os 9 módulos

| # | Módulo | Conceito central |
|---|---|---|
| 00 | Problema de negócio e exploração | perfilagem, integridade referencial |
| 01 | Bronze | ingestão fiel, metadados, rastreabilidade |
| 02 | Silver — dimensões | tipagem, deduplicação, RI, quarentena |
| 03 | Silver — fato conta médica | regras de negócio, quarentena com motivo |
| 04 | Silver — vigências | SCD Tipo 2, precedência `AND`/`OR`, forward fill |
| 05 | Gold | grão, as-of join, métrica de anomalia, surrogate key |
| 06 | Qualidade | painel `dq_metricas`, porta de qualidade, reconciliação |
| 07 | Lakeflow Declarative Pipeline | declarativo vs imperativo, `EXPECT` |
| 08 | Consumo + desafio final | orquestração e análise de causa |

## Notas técnicas

- Cada notebook começa com `USE CATALOG amil_workshop_trilha_tech;` e
  `USE SCHEMA seu_usuario;`. O participante troca essa segunda linha uma vez, e
  todas as consultas do módulo usam nomes simples de tabela.
- O gerador de dados usa `hash()`/`pmod()` (determinístico e reprodutível),
  **não** `rand()`: o mesmo dado em toda execução e em todos os schemas.
- O modelo de dados **não contém PII nem dado clínico**: nem nome, CPF, endereço
  ou diagnóstico do beneficiário. Análise de custo assistencial não precisa deles.
