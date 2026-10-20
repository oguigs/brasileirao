# Brasileirão 2026 com Lakeflow Jobs

Pipeline que calcula as chances de título, Top 5 e rebaixamento do Brasileirão 2026,
construída ao vivo na aula de orquestração. Roda no Databricks Free Edition
(serverless, Unity Catalog, catálogo `workspace`).

## Jobs

| Job | Como dispara | O que faz |
|---|---|---|
| `brasileirao_01_setup` | manual, uma vez | cria schemas e volume e copia os dados |
| `brasileirao_02_historico` | manual | carrega 2012 a 2026, treina o modelo, simula e publica |
| `brasileirao_03_incremental` | chegada de arquivo em `landing/resultados/` | ingere, valida, faz MERGE e recalcula |
| `brasileirao_04_busca_agendada` | dom, seg, qua e qui às 23:40 | busca resultados novos e grava um CSV na landing |
| `brasileirao_05_orquestrador` | manual, parâmetro `modo` | chama o histórico ou o incremental |

Tabela final: `workspace.brasileirao_gold.tabela_final`.

## Como rodar

1. No Databricks: **Workspace > Create > Git folder** com a URL deste repositório.
2. Crie os jobs na tela, como na aula, ou a partir dos arquivos de `resources/jobs`.
3. Rode `brasileirao_01_setup` e depois `brasileirao_02_historico`.
4. Para simular uma rodada, rode o notebook `SOLTAR_ARQUIVO`.

## Estrutura

- `dados/`: histórico 2012-2026, calendário, nomes dos times e arquivos de exemplo
- `src/lib/brasileirao/`: regras do projeto (configuração, modelo, tabela e simulação)
- `src/notebooks/`: uma tarefa de job por notebook (00 a 80)
- `resources/jobs/`: definição dos jobs em YAML
- `SOLTAR_ARQUIVO.py`: simula a chegada de um arquivo de resultados

Dados históricos e odds: football-data.co.uk.
