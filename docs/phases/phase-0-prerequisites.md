# Phase 0 — Prerequisites

**Goal:** Get everything installed, the project folder created, and raw source data loaded
in Databricks. By the end of this phase you have nothing running yet — but you have the
right tools, the right structure, and real data to query.

---

## What you will do

| Step | What | Where |
|---|---|---|
| 0.1 | Install Python tools | your terminal |
| 0.2 | Init the project folder + git | your terminal |
| 0.3 | Create the full folder structure | your terminal (already done) |
| 0.4 | Create catalogs + schemas in Databricks | Databricks SQL editor |
| 0.5 | Create raw tables + insert sample data | Databricks SQL editor |
| 0.6 | Verify row counts | Databricks SQL editor |

---

## 0.1 Install tools

Open a terminal and run:

```bash
python --version
# Must be 3.11 or higher. If not, install via pyenv or brew:
# brew install python@3.11

pip install dbt-databricks==1.8.7
pip install elementary-data==0.14.3
pip install databricks-cli
```

Verify the installs worked:
```bash
dbt --version
# dbt Core: 1.8.x

edr --version
# elementary 0.14.x
```

**Why these versions?**
- `dbt-databricks 1.8.7` — stable release that supports `replace_where` incremental strategy,
  which is the core pattern used in all mart models throughout this project.
- `elementary-data 0.14.3` — the data observability layer added in Phase 13. Installing it now
  means `dbt deps` won't surprise you later.
- `databricks-cli` — needed for the CI/CD bundle deploy step in Phase 15.

---

## 0.2 Init the project folder + git

The project lives at `dataplex-dwh/`. This folder has already been created for you.

If you haven't already, initialise git:

```bash
cd /path/to/dataplex-dwh
git init
git add .gitignore
git commit -m "chore: init project"
```

The `.gitignore` at the root already excludes:
- `dbt/profiles.yml` — contains your Databricks token, never commit this
- `dbt/target/` — compiled SQL artifacts, large and re-generatable
- `dbt/dbt_packages/` — installed packages, re-generatable via `dbt deps`
- `dbt/logs/` — runtime logs

---

## 0.3 Understand the folder structure

```
dataplex-dwh/
├── .gitignore
├── .github/
│   └── workflows/          # CI/CD pipeline (Phase 15)
├── airflow/
│   └── src/
│       └── dwh/            # Airflow DAG files (Phases 8–16)
├── dbt/
│   ├── dbt_project.yml     # Main dbt config — grows phase by phase
│   ├── packages.yml        # dbt package dependencies
│   ├── profiles.yml        # NOT committed — your Databricks connection
│   ├── selectors.yml       # Named model selectors (Phase 7+)
│   ├── seeds/              # CSV lookup tables
│   ├── snapshots/          # SCD snapshots (not used in this POC)
│   ├── macros/
│   │   ├── utils/          # General-purpose macros (Phase 6)
│   │   └── validation/     # Dual-run alias macro (Phase 6/12)
│   └── models/
│       ├── staging/        # Layer 1: thin views over raw sources
│       │   ├── hr/
│       │   ├── marketplace/
│       │   └── payments/
│       ├── intermediate/   # Layer 2: ephemeral enrichment (never materialised)
│       │   ├── hr/
│       │   ├── marketplace/
│       │   └── payments/
│       ├── marts/          # Layer 3: incremental domain tables (the "product")
│       │   ├── hr/
│       │   ├── marketplace/
│       │   └── payments/
│       ├── analytics/      # Layer 4: cross-domain joins (Phase 4)
│       │   └── performance/
│       ├── reporting/      # Layer 5: BI-shaped output (Phase 5)
│       │   └── executive/
│       └── dwh_internal/   # Validation models (Phase 14)
├── sql/
│   ├── phase_0_setup.sql   # Run this in Databricks SQL editor
│   └── adhoc_sql.sql       # Ad-hoc SQL (added in Phase 12)
└── docs/
    └── phases/             # This documentation — one file per phase
```

**The layered model pattern** (staging → intermediate → marts) is the core architectural
decision. Here is why each layer exists:

| Layer | Materialisation | Purpose |
|---|---|---|
| staging | view | Rename and type-cast raw columns. No business logic. Maps 1:1 to a source table. |
| intermediate | ephemeral | Enrich and calculate derived fields. Compiled inline into the mart query — no table is created. |
| marts | incremental table | The "published" output. Aggregated, partitioned, clustered. What downstream users query. |
| analytics | incremental table | Cross-domain joins that no single domain can do alone. |
| reporting | table | BI-layer shape. Column aliases, ordering, 90-day filter. Rebuilt from scratch each run. |
| dwh_internal | view | Validation only. Never queried by downstream users. |

---

## 0.4 Databricks: create catalogs and schemas

Open the **Databricks SQL editor** and run the contents of `sql/phase_0_setup.sql`,
or copy-paste the blocks below section by section.

**Required privilege:** `CREATE CATALOG` on the metastore, or have your Databricks
admin run this step for you.

```sql
-- Dev catalog
CREATE CATALOG IF NOT EXISTS dwh_udev;

-- Prod catalog
CREATE CATALOG IF NOT EXISTS dwh_upro;
```

Then for each catalog, create the schemas:

```sql
USE CATALOG dwh_udev;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS marketplace;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS dwh_internal;
CREATE SCHEMA IF NOT EXISTS seeds;

-- Repeat for dwh_upro (same schema list)
USE CATALOG dwh_upro;
CREATE SCHEMA IF NOT EXISTS staging;
-- ... (same as above)
```

**Why two catalogs?**
- `dwh_udev` = dev/test environment. dbt target `udev` writes here.
- `dwh_upro` = production. dbt target `upro` writes here.
- Keeping them as separate Unity Catalog catalogs means you can apply different
  access policies, spot-check prod without affecting dev, and run CI against udev
  without risk of touching prod.

**Why create all schemas upfront?**
dbt can create schemas automatically but it requires elevated privileges. Creating them
now with `IF NOT EXISTS` means a developer with write-only access to the schemas
can run dbt without needing DDL privileges.

---

## 0.5 Databricks: create raw source tables + insert data

Still in the Databricks SQL editor:

```sql
USE CATALOG dwh_udev;

CREATE TABLE IF NOT EXISTS staging.raw_hr__employees (
    employee_id   BIGINT,
    full_name     STRING,
    department_id INT,
    salary        DECIMAL(10,2),
    hire_date     DATE,
    status        STRING,
    site_id       INT,
    event_date    DATE
) USING DELTA PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS staging.raw_mkt__listings (
    listing_id  BIGINT,
    seller_id   BIGINT,
    category_id INT,
    price       DECIMAL(10,2),
    status      STRING,
    site_id     INT,
    listed_date DATE,
    event_date  DATE
) USING DELTA PARTITIONED BY (event_date);

CREATE TABLE IF NOT EXISTS staging.raw_pay__transactions (
    transaction_id BIGINT,
    buyer_id       BIGINT,
    seller_id      BIGINT,
    listing_id     BIGINT,
    amount         DECIMAL(10,2),
    currency_code  STRING,
    status         STRING,
    site_id        INT,
    event_date     DATE
) USING DELTA PARTITIONED BY (event_date);
```

Insert sample data:

```sql
INSERT INTO staging.raw_hr__employees VALUES
    (1, 'Alice Smith',  10, 75000, '2020-01-15', 'active',     1, '2026-08-18'),
    (2, 'Bob Jones',    20, 65000, '2019-06-01', 'active',     1, '2026-08-18'),
    (3, 'Carol White',  10, 80000, '2021-03-10', 'terminated', 2, '2026-08-18'),
    (4, 'Dave Brown',   30, 55000, '2022-11-20', 'active',     2, '2026-08-19'),
    (5, 'Eve Davis',    20, 90000, '2018-07-04', 'active',     1, '2026-08-19'),
    (6, 'Frank Miller', 10, 72000, '2023-02-14', 'active',     3, '2026-08-20');

INSERT INTO staging.raw_mkt__listings VALUES
    (101, 1, 10, 150.00, 'active',  1, '2026-08-15', '2026-08-18'),
    (102, 2, 20, 299.99, 'sold',    1, '2026-08-10', '2026-08-18'),
    (103, 4, 10,  75.50, 'active',  2, '2026-08-17', '2026-08-19'),
    (104, 5, 30, 499.00, 'active',  1, '2026-08-18', '2026-08-19'),
    (105, 1, 20, 199.00, 'expired', 3, '2026-08-01', '2026-08-20');

INSERT INTO staging.raw_pay__transactions VALUES
    (1001, 3, 1, 102, 299.99, 'EUR', 'completed', 1, '2026-08-18'),
    (1002, 2, 4, 103,  75.50, 'EUR', 'completed', 2, '2026-08-19'),
    (1003, 1, 5, 104, 499.00, 'EUR', 'failed',    1, '2026-08-19'),
    (1004, 5, 1, 101, 150.00, 'EUR', 'completed', 1, '2026-08-20');
```

**Why these 3 event_dates (Aug 18, 19, 20)?**
The incremental mart models use a self-healing lookback window:
`event_date > max(event_date) - 2`. With data across 3 dates, you can run the
incremental build twice and see that:
- First run (full-refresh) loads all 3 dates
- Second run (incremental) only re-processes the last 2 dates — proving the filter works

---

## 0.6 Verify

Run the verification query in the SQL editor:

```sql
SELECT 'raw_hr__employees'     AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_hr__employees
UNION ALL
SELECT 'raw_mkt__listings'     AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_mkt__listings
UNION ALL
SELECT 'raw_pay__transactions' AS table_name, COUNT(*) AS row_count FROM dwh_udev.staging.raw_pay__transactions;
```

Expected result:

| table_name | row_count |
|---|---|
| raw_hr__employees | 6 |
| raw_mkt__listings | 5 |
| raw_pay__transactions | 4 |

---

## Naming conventions used in this project

Understanding these now will make every subsequent phase easier to read.

| Convention | Example | Meaning |
|---|---|---|
| `raw_<domain>__<entity>` | `raw_hr__employees` | Raw source table in `staging` schema |
| `stg_<domain>__<entity>` | `stg_hr__employees` | dbt staging view |
| `int_<domain>__<entity>_<suffix>` | `int_hr__employee_metrics` | dbt intermediate (ephemeral) |
| `<domain>__<entity>` | `hr__employee_summary` | dbt mart table |
| `perf__<entity>` | `perf__fact_daily` | Analytics layer model |
| `exec__<entity>` | `exec__report_daily_kpis` | Reporting layer model |
| `seed_<domain>__<entity>_lkp` | `seed_hr__department_lkp` | dbt seed (CSV lookup) |
| `<domain>__<entity>_serverless` | `pay__transaction_summary_serverless` | Dual-run shadow table (Phase 12) |

Double underscore (`__`) separates the domain prefix from the entity name.
This is the dbt community convention and makes it easy to filter by domain in the UI.

---

## What's next

Phase 0 is complete when:
- [x] All tools installed (`dbt --version` works)
- [x] `dataplex-dwh/` folder structure exists
- [x] `.gitignore` is in place
- [ ] `dwh_udev` and `dwh_upro` catalogs exist in Databricks
- [ ] All 9 schemas exist in both catalogs
- [ ] 3 raw tables exist in `dwh_udev.staging`
- [ ] Row counts match: 6 / 5 / 4

**Next:** [Phase 1 — First Working Model (HR only)](./phase-1-hr-domain.md)
