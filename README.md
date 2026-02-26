# Projeto de Engenharia de Dados (Bronze / Silver / Gold)

## Estrutura sugerida

```text
project-root/
│
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── bronze/
│   │   ├── ingest_bronze.py
│   │   └── fetch_holidays.py
│   ├── silver/
│   │   ├── transform_silver.py
│   │   └── sla_calculation.py
│   ├── gold/
│   │   ├── build_gold.py
│   │   └── build_sla_reports.py
│
├── requirements.txt
├── README.md
└── .gitignore
```

## Fluxo do pipeline

1. **Bronze**: coleta dados brutos do Jira e salva em `data/bronze/`.
2. **Silver**: transforma, normaliza e calcula SLA por chamado; salva em `data/silver/silver_issues.parquet`.
3. **Gold**: gera visão analítica final; salva em `data/gold/gold_issues_full.parquet`.
4. **Reports**: gera CSVs analíticos a partir de `data/gold/gold_issues_full.parquet`.

## Arquitetura lógica do pipeline

O pipeline segue o padrão de camadas **Bronze → Silver → Gold → Reports**, separando responsabilidades para facilitar manutenção e evolução:

- **Bronze (ingestão)**
	- Coleta dados do Jira sem transformação analítica.
	- Armazena os dados brutos em um arquivo json e salva em `data/bronze/` para reprocessamento posterior.

- **Silver (padronização e enriquecimento)**
	- Explode estruturas aninhadas (assignee e timestamps) em formato tabular.
	- Normaliza tipos de dados (strings e datetimes UTC).
	- Remove registros incompletos (ex.: sem `resolved_at`).
	- Calcula métricas de SLA por chamado (`resolution_hours`, `resolution_days`, `sla_expected_hours`, `sla_met`).

- **Gold (curadoria analítica)**
	- Aplica filtro final para excluir chamados com status `Open`.
	- Ordena e publica a tabela analítica final em `data/gold/gold_issues_full.parquet`.

- **Reports (saídas para consumo)**
	- Gera relatórios CSV com SLA médio por analista e por tipo de chamado a partir da Gold.

## Descrição lógica do cálculo de SLA

O cálculo de SLA é feito em **horas úteis**, considerando calendário de trabalho:

1. Para cada chamado, usa `created_at` (início) e `resolved_at` (fim).
2. Conta apenas horas entre **08:00 e 18:00**.
3. Desconsidera **sábados, domingos** e **feriados nacionais** (arquivo `data/bronze/holidays.json`).
4. Soma as horas úteis em `resolution_hours` e deriva `resolution_days = resolution_hours / 8`.
5. Define o SLA esperado por prioridade:
	 - `High` = 24 horas úteis
	 - `Medium` = 72 horas úteis
	 - `Low` = 120 horas úteis
6. Marca conformidade com `sla_met = resolution_hours <= sla_expected_hours`.

## Dicionário de dados

### Tabela final Gold: `data/gold/gold_issues_full.parquet`

| Coluna | Tipo lógico | Descrição |
|---|---|---|
| `id` | string | Identificador do chamado no Jira. |
| `issue_type` | string | Tipo do chamado (bug, task, etc.). |
| `assignee_id` | string | Identificador do analista responsável. |
| `assignee_name` | string | Nome do analista responsável. |
| `priority` | string | Prioridade do chamado (`High`, `Medium`, `Low`). |
| `created_at` | datetime (UTC) | Data/hora de criação do chamado. |
| `resolved_at` | datetime (UTC) | Data/hora de resolução do chamado. |
| `resolution_hours` | float | Tempo de resolução em horas úteis. |
| `sla_expected_hours` | float/int | Meta de SLA esperada em horas úteis, conforme prioridade. |
| `sla_met` | boolean | Indicador de cumprimento do SLA (`true`/`false`). |

### Relatório: `data/gold/avg_sla_by_analyst.csv`

| Coluna | Tipo lógico | Descrição |
|---|---|---|
| `assignee_id` | string | Identificador do analista (valores nulos tratados como `Unassigned`). |
| `assignee_name` | string | Nome do analista (valores nulos tratados como `Unassigned`). |
| `issues_quantity` | int | Quantidade de chamados resolvidos pelo analista. |
| `avg_sla_hours` | float | Média de `resolution_hours` por analista. |

### Relatório: `data/gold/avg_sla_by_issue_type.csv`

| Coluna | Tipo lógico | Descrição |
|---|---|---|
| `issue_type` | string | Tipo de chamado. |
| `issues_quantity` | int | Quantidade de chamados resolvidos por tipo. |
| `avg_sla_hours` | float | Média de `resolution_hours` por tipo de chamado. |

## Como executar

Com o ambiente virtual ativado, execute na raiz do projeto:

```bash
python src/bronze/fetch_holidays.py
python src/silver/transform_silver.py
python src/gold/build_gold.py
```

Ou execute tudo em um único comando:

```powershell
.\run_pipeline.ps1
```

Ou, de forma cross-platform (Windows/Linux/macOS):

```bash
python run_pipeline.py
```

Se quiser apenas testar a ingestão Bronze:

```bash
python src/bronze/ingest_bronze.py
```

Se quiser gerar apenas os relatórios CSV de SLA (sem rebuild completo):

```bash
python src/gold/build_sla_reports.py
```
