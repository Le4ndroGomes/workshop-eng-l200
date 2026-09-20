# Arquitetura do Workshop — Custo Assistencial (Amil, L200)

> Todos os dados utilizados no workshop são sintéticos e não representam pacientes reais.

## Visão geral

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SISTEMAS DE ORIGEM (simulados pelo setup)                                   │
│                                                                              │
│   SGR — Rede Credenciada     SGB — Beneficiários     SIA — Contas Médicas     │
│   tb_estabelecimento         tb_plano                tb_procedimento          │
│   tb_prestador               tb_beneficiario         tb_conta_medica          │
│   au_prestador (auditoria)                                                    │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  setup_participantes.py (determinístico, hash/pmod)
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  LANDING  —  raw_sgr_* / raw_sgb_* / raw_sia_*                               │
│  Cópia fiel da origem, com todos os defeitos preservados                     │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  módulo 01 — CTAS + metadados de ingestão
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  BRONZE  —  brz_estabelecimento, brz_prestador, brz_prestador_auditoria,     │
│             brz_plano, brz_beneficiario, brz_procedimento, brz_conta_medica  │
│  Regra de ouro: NADA é corrigido. Só se acrescenta _dt_ingestao.             │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  módulos 02–04 — tipagem, dedup, RI, regras, SCD2
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  SILVER                                            QUARENTENA                │
│   slv_prestador_estabelecimento   (dimensão)        qua_prestador_orfao       │
│   slv_beneficiario_plano          (dimensão)        qua_conta_invalida        │
│   slv_conta_medica                (fato)              + motivo_quarentena    │
│   slv_prestador_evento            (linha do tempo)    + valor retido         │
│   slv_prestador_vigencia          (SCD Tipo 2)                               │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │  módulo 05 — agregação + as-of join + métricas
                               ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  GOLD  —  gold_custo_utilizacao_prestador                                    │
│  Grão: prestador × competência                                               │
│  VL_CUSTO_TOTAL · QT_CONTAS · QT_BENEFICIARIOS · TX_UTILIZACAO_CAPACIDADE     │
│  PCT_GLOSA · IDX_CUSTO_TABELA · IDX_CUSTO_VS_PARES · FL_ANOMALIA_CUSTO        │
└──────────────────────────────┬───────────────────────────────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌───────────────────────────┐      ┌────────────────────────────────────────┐
│ QUALIDADE (módulo 06)     │      │ CONSUMO (módulo 08)                    │
│ dq_metricas + porta de    │      │ custo por região / especialidade /     │
│ qualidade (severidade     │      │ plano · desafio final da Diretoria     │
│ erro = pipeline para)     │      │ Financeira                             │
└───────────────────────────┘      └────────────────────────────────────────┘
```

## Caminho alternativo — declarativo (módulo 07)

O mesmo pipeline, reescrito como **Lakeflow Declarative Pipeline**. As tabelas
usam prefixo `sdp_` para conviver com as imperativas no mesmo schema:

```
raw_* ──► sdp_slv_prestador_estabelecimento ──┬─► sdp_slv_conta_medica ──┐
          sdp_slv_beneficiario_plano ─────────┘                          │
                     └─► sdp_slv_prestador_evento ─► sdp_slv_prestador_vigencia ─┤
                                                                                  ▼
                                                            sdp_gold_custo_utilizacao
```

Nenhuma ordem é declarada: o Lakeflow a deduz das referências `LIVE.`. As regras
de qualidade viram `CONSTRAINT ... EXPECT`, com painel de expectativas nativo.

## Isolamento por participante

Cada pessoa trabalha em seu próprio schema dentro do catálogo
`amil_workshop_trilha_tech`:

```sql
DECLARE OR REPLACE VARIABLE meu_schema STRING
  DEFAULT 'amil_workshop_trilha_tech.' || replace(split(current_user(), '@')[0], '.', '_');
-- uso: IDENTIFIER(meu_schema || '.minha_tabela')
```

Os dados são gerados com `hash()`/`pmod()` — **nunca** `rand()` — portanto são
idênticos em toda execução e em todos os schemas. Isso é o que torna o gabarito,
as conferências e o desafio final reprodutíveis.

## Privacidade desde o desenho

O modelo não contém nome, CPF, endereço, telefone nem qualquer dado clínico do
beneficiário — apenas identificador interno, carteira sintética, plano, UF,
região, faixa etária ANS e sexo. Custo assistencial é analisável sem dado
sensível: a arquitetura é a demonstração prática desse ponto, e a LGPD aparece
como consciência de contexto, não como tema técnico central.
