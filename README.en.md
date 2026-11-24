# sales-transformacoes-fabric

[Português](README.md) · **English**

End-to-end data engineering project on **Microsoft Fabric**: from a SQL Server source to a **Power BI report that answers business questions**. **Medallion** architecture (bronze, silver, gold), three environments (**dev, feature and prod**) and change promotion through **Git, pull requests and deployment pipelines**. All data is fictitious; the business vocabulary (table and measure names) is in Portuguese.

Load configuration does not come from a spreadsheet: it comes from a control table (`db_control`, a Fabric SQL Database). Loading a new table takes a new row in that table, with no code change.

## What is delivered

| Stage | Deliverable | Where |
|---|---|---|
| Ingestion and layers | Staging, Bronze, Silver and **Gold** (star schema) | `Processing/`, `Storage/Lakehouses` |
| Data quality | `nb_gold_checks`: 28 checks (keys, reconciliation with Silver, orphan sales, calendar) | `Processing/Notebooks` |
| Semantic model | `sm_vendas_directlake`: Direct Lake on OneLake, 6 tables, **80 DAX measures** | `Storage/Semantic Models` |
| Report | `rpt_vendas`: 3 pages that answer business questions | `Visualization/Reports` |
| Environments | dev, feature and prod with Git, pull requests and a deployment pipeline | see below |

## Data architecture

<p align="center">
  <img src="docs/img/arquitetura-medalhao.svg" alt="Medallion architecture: SQL Server, Staging, Bronze, Silver, Gold, semantic model and report, with governance at the base (diagram labels in Portuguese)" width="100%">
</p>

The pipelines (`pl_Staging`, `pl_bronze`, `pl_silver`, `pl_gold`) are orchestrated by `pl_master` and driven by `db_control`.

| Layer | What it holds | How it is built |
|---|---|---|
| Staging | raw copy of the source, as parquet | `pl_Staging` (Copy activity) |
| Bronze | Delta tables mirroring the source | `pl_bronze` + `nb_bronze` |
| Silver | cleaned data, dimensions and fact | `pl_silver` + `nb_silver_*` notebooks |
| Gold | star schema: `fato_vendas` (sales fact) + `dim_clientes`, `dim_produtos`, `dim_vendedores`, `dim_empresas`, `dim_data` | `pl_gold` + `nb_gold` (one generic notebook, driven by `db_control`) |

Reusable functions, such as `safe_merge` (idempotent incremental load), live in `nb_functions`. `nb_delta_optimization` maintains the Delta tables (OPTIMIZE with V-Order and VACUUM) using native Spark SQL only.

## The report: business questions, not just charts

Each page answers **one** question and follows the same reading order: a **title that states the conclusion**, the **cause**, the filter context, **KPIs with a reference** (previous month and same month last year), the main chart and the **next action**, with a rule computed from the data. Titles are DAX measures, so they update themselves when the data changes.

**1. Monthly result:** did sales go up or down, and why?

<p align="center"><img src="docs/img/relatorio-p1-resultado-do-mes.png" alt="Monthly result page: conclusion title, four KPIs with reference, variance bridge by salesperson and next action" width="100%"></p>

**2. Concentration:** who drives the revenue?

<p align="center"><img src="docs/img/relatorio-p2-concentracao.png" alt="Concentration page: salesperson Pareto, products ranked by sales and how many make up 80% of sales" width="100%"></p>

**3. Reversals and discounts:** where are sales being reversed, and when did discounts spike?

<p align="center"><img src="docs/img/relatorio-p3-estornos-e-desconto.png" alt="Reversals and discounts page: monthly reversal rate with an alert threshold, monthly discount rate and reversed amount by salesperson" width="100%"></p>

Design decisions:

- **No invented numbers.** The data has no targets or costs, so every reference is the previous period. Rule thresholds come from the data itself (for example, mean + 1 standard deviation) or are flagged as "example threshold".
- **Last complete month.** Data ends on 2020-10-16; comparing a 16-day October with September would show a false 44% drop.
- **Contrast measured, not eyeballed.** Every color was tested with the WCAG formula (text ≥ 4.5:1, charts ≥ 3:1). That is why the coral accent gets darker when used as text.
- **Checked against DAX.** On-screen totals match independent DAX and SQL queries (for example, the variance bridge adds up exactly to the month's change).

## Environments and change flow

<p align="center">
  <img src="docs/img/ambientes-fluxo.svg" alt="Change flow: feature, pull request, main on GitHub, Git update, dev, deployment with a note and prod (diagram labels in Portuguese)" width="100%">
</p>

- **Feature:** where each developer works, connected to their own branch. Nothing goes straight to `main`.
- **Dev:** connected to `main`. Receives what was approved in the pull request.
- **Prod:** no Git connection. Changes arrive only through the **deployment pipeline** (`pl_deploy_sales`), as selective deployments with a **note** (what, why, commit, verification).
- Git stores **definitions** (notebooks, pipelines, schemas, model, report), never **data**. Each environment is filled by running its own pipelines.
- A fix in production also starts on a branch, goes through the pull request and dev, and only then reaches prod.

## Per-environment parameters

- **Pipelines:** the **Variable Library** (`vl_solucoes_fabric`) has one value set per environment (`dev`, `feature`, `prd`). The active set is a workspace setting and **is not stored in Git**.
- **Semantic model:** Direct Lake **does not rebind itself** to the next environment's lakehouse. The source uses two M parameters (`p_workspace_id` and `p_lakehouse_id`); in prod, a deployment pipeline **parameter rule** points the model to the prod lakehouse. This was proven by re-reading the model definition after each deployment.
- **Report:** deployed together with the model, it rebinds to the model in the same environment.

## Creating a new environment

1. **Create the workspace** on the Fabric capacity.
2. **Connect it to Git** (dev and feature) or **receive it through the deployment pipeline** (prod).
3. **Activate the Variable Library value set** for that environment.
4. **Run the `db_control` seed** (see below).
5. **Run the pipelines** one at a time: `pl_Staging`, `pl_bronze`, `pl_silver`, `pl_gold`.
6. **Check data quality** with `nb_gold_checks`.

## `db_control` seed

`seeds/PipelineControl.seed.sql` inserts the control rows (40 today: Staging 13, Bronze 13, Silver 8, Gold 6) with a `MERGE` on the **layer + entity** key, so it can run many times without duplicating. The source database name is **not in the file**: it is passed as a parameter at run time.

```python
from tools.run_seed import run_seed
run_seed("<workspace name>", "<source database name>")
```

Requirements: Python, `pyodbc`, ODBC Driver 18 for SQL Server and an Azure CLI login (`az login`). `tools/test_run_seed.py` runs the seed twice and checks that the row count does not change.

## New table = new rows in the seed

1. Add the table's rows to `seeds/PipelineControl.seed.sql` (one per layer).
2. For a Gold table, add one function to the `TRANSFORMS` dictionary in `nb_gold`.
3. Open a pull request and promote through the flow above; run the seed in each environment, then the pipelines.

The pipelines iterate over `db_control` with a `ForEach`; the new row is picked up automatically.

## Resilience and data quality

- `retry` on activities that depend on network or capacity, and `batchCount` to stay within the Spark session limit.
- Idempotent loads: running again does not duplicate data.
- **Quality as a test:** `nb_gold_checks` was written **before** the Gold layer (it failed first and passed afterwards) and runs in any environment.
- Fault-tolerant Delta maintenance: one failing table does not block the others, and the notebook fails at the end listing which ones broke.

## Security and good practices

- Public repository: **no passwords, tokens, connection strings or real system names**.
- Least privilege: few admins; every access has an owner and a reason.
- Read-only users for source connections.
- Every production deployment has a note and a log entry.

## Repository layout

<p align="center">
  <img src="docs/img/estrutura-repositorio.svg" alt="Repository layout: Environment, Processing, Storage and Visualization (Fabric items) plus seeds, tools and docs (supporting files)" width="100%">
</p>

```
Environment/     Environment and Variable Library
Processing/      Notebooks, pipelines and dataflow
Storage/         Lakehouses, semantic model and control database
Visualization/   Power BI report (PBIR)
seeds/           db_control seed
tools/           Seed script and test
docs/img/        Diagrams and report screenshots
```

Legacy items from the original project (`sm_solucoes-fabric_*`, `nb_semantic_models`, `df_date`) are still in the repository and will be retired after a cost comparison in Capacity Metrics.

## Official documentation

- [Git integration in Fabric](https://learn.microsoft.com/fabric/cicd/git-integration/intro-to-git-integration)
- [Deployment pipelines](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines)
- [Deployment rules](https://learn.microsoft.com/fabric/cicd/deployment-pipelines/create-rules)
- [Variable libraries](https://learn.microsoft.com/fabric/cicd/variable-library/variable-library-overview)
- [Medallion architecture](https://learn.microsoft.com/fabric/onelake/onelake-medallion-lakehouse-architecture)
- [Direct Lake](https://learn.microsoft.com/fabric/fundamentals/direct-lake-overview) and [M parameters with deployment pipelines](https://learn.microsoft.com/fabric/fundamentals/direct-lake-develop#use-m-parameters-with-deployment-pipelines)
- [PBIR report format](https://learn.microsoft.com/power-bi/developer/projects/projects-report)
