# dataplex-dwh — Dataplex DWH (Incremental Build POC)

A production-pattern Data Warehouse built on Databricks + dbt + Airflow, constructed
phase by phase so every piece is understood before the next is added.

---

## What this project builds

A multi-domain DWH with three business domains (HR, Marketplace, Payments), a
cross-domain analytics layer, a BI reporting layer, full Airflow orchestration,
data observability via Elementary, dual-run serverless migration, and a cost
monitoring dashboard.

### Final architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              dwh_raw  (Bronze — ingestion writes here)          │
│  hr.raw_hr__employees                                           │
│  mkt.raw_mkt__listings                                          │
│  pay.raw_pay__transactions                                      │
└───────────────────────┬─────────────────────────────────────────┘
                        │ dbt
                        ▼
┌──────────────────────────────────────────────┐
│               STAGING LAYER (views)          │
│  stg_hr__employees                           │
│  stg_mkt__listings                           │
│  stg_pay__transactions                       │
└────────────────────┬─────────────────────────┘
                     │ (ephemeral intermediate)
                     ▼
┌──────────────────────────────────────────────┐
│               MART LAYER (incremental)       │
│  hr__employee_summary           [hr schema]  │
│  mkt__listing_summary    [marketplace schema]│
│  pay__transaction_summary  [payments schema] │
└──────────────┬───────────────────────────────┘
               │ cross-domain join
               ▼
┌──────────────────────────────────────────────┐
│            ANALYTICS LAYER (incremental)     │
│  perf__dim_category                          │
│  perf__fact_daily            [analytics schema]│
└──────────────┬───────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────┐
│            REPORTING LAYER (table)           │
│  exec__report_daily_kpis   [reporting schema]│
└──────────────────────────────────────────────┘
```

### Airflow DAG shape (final — after Phase 16)

```
poc.dbt.daily  (runs 05:00 CET daily)
  │
  ├── dbt_daily           AP cluster   runs everything except domain tags
  │
  ├── dbt_hr              serverless   --selector hr
  ├── dbt_marketplace     serverless   --selector marketplace
  ├── dbt_analytics_layer serverless   --selector analytics_layer
  └── dbt_payments        serverless   --selector payments
          │
          └── dbt_elementary_alert_tests  (join point — waits for all groups)
                ├── elementary_freshness_results
                └── elementary_anomaly_results
```

---

## Technology stack

| Tool | Version | Role |
|---|---|---|
| dbt-databricks | 1.8.7 | Transform + test layer |
| elementary-data | 0.14.3 | Data observability + alerts |
| Apache Airflow | 2.x | Orchestration |
| Databricks (Delta Lake) | Unity Catalog | Storage + compute |
| GitHub Actions | — | CI/CD |

---

## The 17 phases

| Phase | What you build | Key concept introduced |
|---|---|---|
| [0](docs/phases/phase-0-prerequisites.md) | Tools + folder + raw tables | Project structure, Unity Catalog |
| [1](docs/phases/phase-1-hr-domain.md) | HR domain end-to-end | staging→intermediate→mart, `replace_where` incremental |
| [2](docs/phases/phase-2-marketplace-domain.md) | Marketplace domain | Second domain, same pattern repeats |
| [3](docs/phases/phase-3-payments-domain.md) | Payments domain | Third domain, seeds for lookups |
| [4](docs/phases/phase-4-analytics-layer.md) | Cross-domain analytics | `ref()` across domains, `insert_overwrite` |
| [5](docs/phases/phase-5-reporting-layer.md) | Reporting layer | BI-shaped output, `table` materialisation |
| [6](docs/phases/phase-6-macros.md) | Macros | `is_not_production`, `generate_alias_name` |
| [7](docs/phases/phase-7-selectors.md) | Selectors | Named model selections for Airflow |
| [8](docs/phases/phase-8-airflow-v1.md) | Airflow v1: daily only | First working DAG, AP cluster |
| [9](docs/phases/phase-9-airflow-v2.md) | Airflow v2: HR serverless | First serverless group, selector exclusion |
| [10](docs/phases/phase-10-airflow-v3.md) | Airflow v3: marketplace | Adding a group = one dict entry |
| [11](docs/phases/phase-11-airflow-v4.md) | Airflow v4: analytics | Grouping analytics + reporting together |
| [12](docs/phases/phase-12-airflow-v5.md) | Airflow v5: dual-run payments | Shadow tables, alias macro, two groups from one entry |
| [13](docs/phases/phase-13-airflow-v6.md) | Airflow v6: elementary | Join point pattern, wiring all groups |
| [14](docs/phases/phase-14-dwh-internal.md) | DWH internal validation | Var-gated models, AP vs serverless comparison |
| [15](docs/phases/phase-15-cicd.md) | CI/CD | GitHub Actions: compile + lint on PR, deploy on merge |
| [16](docs/phases/phase-16-cutover.md) | Payments cutover | Moving from dual-run to serverless-only |
| [17](docs/phases/phase-17-cost-dashboard.md) | Cost dashboard | Databricks system tables, DBU concepts |

---

## Incremental strategy — how it works

All mart models use `replace_where` with a 2-day self-healing window:

```
Normal (nightly) run:
  Replaces partitions where event_date > max(event_date) - 2

Backfill run (explicit date range):
  dbt build --vars '{"start_of_backfill_window":"2026-08-18","end_of_backfill_window":"2026-08-19"}'
  Replaces only those two partitions

Full refresh:
  dbt build --full-refresh
  Drops and rebuilds the entire table
```

The 2-day window is a self-healing mechanism: if yesterday's run had a data quality
issue and you need to re-run today, the window automatically picks up yesterday's
partitions again without any manual backfill command.

---

## Environment targets

| dbt target | Databricks catalog | Compute |
|---|---|---|
| `udev` | dwh_udev | SQL Warehouse (classic) |
| `udev_serverless` | dwh_udev | SQL Warehouse (serverless) |
| `upro` | dwh_upro | SQL Warehouse (classic) |
| `upro_serverless` | dwh_upro | SQL Warehouse (serverless) |

The `profiles.yml` file (not committed — see `dbt/profiles.yml.example`) holds all four targets.
The Airflow DAG reads the current environment from the `DBT_ENVIRONMENT` env var and
selects the appropriate target automatically.

---

## Quick start

```bash
# 1. Install tools
pip install dbt-databricks==1.8.7 elementary-data==0.14.3

# 2. Configure connection (copy and fill in your values)
cp dbt/profiles.yml.example dbt/profiles.yml

# 3. Install dbt packages
cd dbt && dbt deps

# 4. Run HR domain (first working model)
dbt seed --target udev
dbt build --target udev --select tag:hr --full-refresh

# 5. Verify
# SELECT * FROM dwh_udev.hr.hr__employee_summary;
```

---

## Repository layout

```
dataplex-dwh/
├── README.md                       ← you are here
├── .gitignore
├── .github/workflows/ci.yml        ← Phase 15
├── airflow/src/dwh/
│   ├── template_lib.py             ← Phase 8
│   ├── project_lib.py              ← Phase 8
│   └── dbt.py                      ← Phases 8–16 (grows each phase)
├── dbt/
│   ├── dbt_project.yml             ← grows each phase
│   ├── packages.yml
│   ├── profiles.yml.example
│   ├── selectors.yml               ← Phase 7+
│   ├── seeds/
│   ├── macros/
│   └── models/
├── sql/
│   ├── phase_0_setup.sql           ← Phase 0 Databricks setup
│   └── adhoc_cleanup.sql           ← drops old raw tables from dwh_udev.staging
└── docs/
    ├── spec.md                     ← master build spec (reference)
    └── phases/                     ← one detailed doc per phase
```
