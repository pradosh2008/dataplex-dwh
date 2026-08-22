# dbt Fundamentals

Everything learned end-to-end while building this project. Updated as new concepts are introduced.

---

## What dbt is (and isn't)

dbt (data build tool) is a **transformation** tool. It does not move data — it transforms data that already exists in your warehouse using SQL.

```
Source system → [ingestion tool] → raw tables → [dbt] → staging/intermediate/mart tables
```

dbt's job: read from raw tables, apply transformations, write output tables/views back into the warehouse. Everything runs inside Databricks — your laptop just sends the SQL.

---

## Project structure

```
dbt/
  dbt_project.yml     ← central config (profile, folder paths, materialisation defaults)
  profiles.yml        ← connection config (host, warehouse, token) — gitignored
  packages.yml        ← external package dependencies
  package-lock.yml    ← auto-generated lock file after dbt deps
  macros/             ← custom Jinja macros that override dbt defaults
  models/
    staging/          ← layer 1: rename/clean raw data → views
    intermediate/     ← layer 2: business logic/enrichment → ephemeral
    marts/            ← layer 3: aggregated output tables → incremental
    analytics/        ← layer 4: cross-domain analytics
    reporting/        ← layer 5: exec-level reporting
  seeds/              ← static CSV lookup tables
  snapshots/          ← SCD Type 2 history tracking (not used yet)
  tests/              ← custom test SQL files
```

---

## profiles.yml — connecting to the warehouse

```yaml
poc_dwh:             # profile name — must match `profile:` in dbt_project.yml
  target: udev       # default output — used when no --target flag passed
  outputs:
    udev:            # output name
      type: databricks
      host: <workspace>.cloud.databricks.com
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: <PAT>
      catalog: dwh_udev    # Unity Catalog catalog — where dbt writes output
      schema: staging      # fallback schema (overridden per layer in dbt_project.yml)
      threads: 8           # parallel model execution
      connect_retries: 5   # retry on cold warehouse start
      connect_timeout: 60
```

**Multiple outputs** — you add more outputs when you need to target different warehouses/catalogs. Phase 9 adds `udev_serverless`, `upro`, `upro_serverless`.

**`target: udev`** — sets the default. `dbt build` uses it. `dbt build --target upro` overrides it.

**Databricks PAT format** — valid Databricks PATs start with `dapi`. If a generated token doesn't start with `dapi`, it's the wrong token type.

---

## dbt_project.yml — project configuration

```yaml
name: poc_dwh
profile: poc_dwh          # links to profiles.yml

models:
  poc_dwh:
    staging:
      +schema: staging    # + means "apply to all models in this folder"
      +materialized: view
      hr:
        +tags: ['hr']     # all models under staging/hr/ get tagged 'hr'

    intermediate:
      +schema: intermediate
      +materialized: ephemeral

    marts:
      hr:
        +schema: hr
        +materialized: incremental
        +tags: ['hr']
```

**`+` prefix** — applies the config to the folder and all subfolders. Without `+`, dbt treats it as a model name.

**Individual models override project defaults** — add `{{ config(materialized='view') }}` inside a model to override the folder-level setting.

---

## Materialisation types

| Type | Stored? | How | When to use |
|---|---|---|---|
| `view` | No | SQL definition only | Staging — just renames, no storage cost |
| `table` | Yes | Full drop + rebuild every run | Small reference tables, rarely changes |
| `ephemeral` | No | Inlined as CTE into parent model | Intermediate logic nobody queries directly |
| `incremental` | Yes | Only processes new/changed rows | Mart tables — large, grows over time |

**Ephemeral detail:** dbt takes the model's SQL and pastes it as a CTE inside whichever model calls `{{ ref() }}` on it. The warehouse sees one combined query — no intermediate storage.

**Incremental detail:** on first run, builds the full table. On subsequent runs, only processes rows matching the `replace_where` predicate. Much cheaper than rebuilding everything nightly.

---

## `{{ source() }}` and `{{ ref() }}`

Never hardcode table names. Use these two functions instead.

### `{{ source('source_name', 'table_name') }}`
References a raw source table declared in `sources.yml`. dbt resolves it to the fully qualified table name at runtime.

```sql
select * from {{ source('hr_raw', 'raw_hr__employees') }}
-- resolves to: dwh_raw.hr.raw_hr__employees
```

Benefits:
- Change location in one place (sources.yml) — all models pick it up
- dbt tracks lineage (raw → staging → mart)
- Enables `dbt source freshness` checks

### `{{ ref('model_name') }}`
References another dbt model or seed. dbt uses this to build the execution order (DAG).

```sql
select * from {{ ref('stg_hr__employees') }}
select * from {{ ref('seed_hr__department_lkp') }}
```

If model A `ref`s model B, dbt always runs B before A. You never need to manage execution order manually.

---

## The DAG (Directed Acyclic Graph)

dbt builds a dependency graph from all `ref()` and `source()` calls:

```
dwh_raw.hr.raw_hr__employees  (source)
        ↓
stg_hr__employees              (staging view)
        ↓
int_hr__employee_metrics       (ephemeral — inlined)
        ↓
hr__employee_summary           (incremental mart)
```

dbt executes in this order automatically. With `threads: 8`, independent branches run in parallel.

---

## Schema naming — generate_schema_name macro

**Default dbt behaviour:** final schema = `target.schema` + `_` + `+schema`

So with `target.schema = staging` and `+schema: hr`:
- Default → `staging_hr` ❌
- With override → `hr` ✓

**Why dbt does this by default:** for teams where multiple developers share one warehouse. Prefixing prevents `alice` and `bob` both writing to the same `hr` schema.

**Why we override it:** we use separate catalogs for isolation (`dwh_udev` = dev, `dwh_upro` = prod). Catalog is our isolation boundary — we want clean schema names.

**The macro** (`macros/generate_schema_name.sql`):
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

If a custom schema is set → use it directly. If not → fall back to `target.schema`.

---

## Seeds

CSV files in `dbt/seeds/` that dbt loads as tables into the warehouse. Used for small, static lookup data that doesn't come from a source system.

```bash
dbt seed --target udev
```

Seeds are referenced in models with `{{ ref('seed_name') }}` just like any other model.

**Naming convention:** `seed_<domain>__<entity>_lkp.csv`

Seeds are kept separate from domain tags — run `dbt seed` independently when data changes.

---

## Tests

dbt tests are SQL assertions that run after models. If a test query returns rows, the test fails.

**Schema tests** (defined in YML):
```yaml
columns:
  - name: employee_id
    data_tests: [not_null]      # fails if any NULL
  - name: status
    data_tests:
      - accepted_values:
          values: ['active', 'terminated']  # fails if any other value
```

**Source tests** — same syntax, applied to raw source tables before models run.

**`dbt build` runs tests right after each model** — if staging tests fail, dbt skips the downstream mart. This prevents bad data propagating.

---

## Incremental strategy — replace_where

Used in all mart models. The pattern:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=["event_date > date_sub((select max(event_date) from " ~ this ~ "), 2)"]
    )
}}

{% if is_incremental() %}
    where event_date > date_sub((select max(event_date) from {{ this }}), 2)
{% endif %}
```

**How it works:**
1. `is_incremental()` returns `true` if the table already exists (not first run)
2. On incremental run: WHERE clause filters to last 2 days only
3. `replace_where` deletes rows matching the predicate, then inserts fresh rows for that window
4. Result: **self-healing** — late-arriving or corrected source data gets reprocessed automatically

**On first run** (`--full-refresh`): `is_incremental()` = false, WHERE clause skipped, all data loaded.

**`{{ this }}`** — dbt built-in, resolves to the current model's fully qualified table name.

---

## Backfill pattern

Reprocess a specific date range without touching the rest of the table:

```bash
dbt build --select tag:hr \
  --vars '{"start_of_backfill_window": "2026-08-01", "end_of_backfill_window": "2026-08-31"}'
```

The model detects these vars and uses them instead of the 2-day lookback:

```sql
{% if var("start_of_backfill_window", false) and var("end_of_backfill_window", false) %}
    where event_date between '{{ var("start_of_backfill_window") }}' and '{{ var("end_of_backfill_window") }}'
{% endif %}
```

---

## Commands reference

| Command | What runs |
|---|---|
| `dbt deps` | Install packages from `packages.yml` into `dbt_packages/` |
| `dbt seed` | Load CSV seeds into warehouse |
| `dbt run` | Run models only |
| `dbt test` | Run tests only |
| `dbt build` | Seeds + models + tests in dependency order |
| `dbt build --full-refresh` | Drop and recreate all incremental models |
| `dbt build --select tag:hr` | Only models tagged `hr` |
| `dbt source freshness` | Check if source data is stale |
| `dbt docs generate` | Build lineage docs site |

**Day-to-day rule:** always use `dbt build`, not `dbt run`.

---

## Selectors

Control which models run:

```bash
--select tag:hr                    # all models tagged hr
--select stg_hr__employees         # one specific model
--select staging/hr                # all models in a folder
--select tag:hr+                   # hr models + all downstream
--select +hr__employee_summary     # hr__employee_summary + all upstream
--select tag:hr --exclude tag:slow # hr models except those tagged slow
```

---

## packages.yml and dbt deps

External packages extend dbt with utility macros and models.

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: elementary-data/elementary
    version: [">=0.14.0", "<1.0.0"]
```

`dbt deps` downloads them into `dbt/dbt_packages/` (gitignored). Run it once after cloning the repo or when packages change.

**Version ranges:** `[">=1.0.0", "<2.0.0"]` = any 1.x release. Protects against breaking changes in v2 while allowing patch updates.

---

## Elementary package

Observability layer — automatically logs dbt run metadata into the warehouse after every build.

- `on-run-end: elementary.on_run_end()` in `dbt_project.yml` triggers it
- Creates 30 tables/views in `dwh_udev.elementary`: `dbt_run_results`, `dbt_models`, `elementary_test_results` etc.
- **One-time setup required:** `dbt run -s elementary --target udev --full-refresh` (run once to create the tables)
- Until setup is run, the hook fires but skips silently — doesn't break anything
- Schema is configured by putting `elementary: +schema: elementary` under the top-level `models:` key in `dbt_project.yml`

**Common pitfall:** If `dbt_project.yml` has two separate `models:` top-level keys, YAML silently discards the first. Always keep all model configs under one `models:` block.

Used in Phase 13 for alert wiring. For now it's installed but dormant.

---

## Jinja templating

dbt models are SQL + Jinja. Jinja blocks:

| Syntax | Purpose |
|---|---|
| `{{ expression }}` | Output a value — `{{ ref('model') }}`, `{{ this }}`, `{{ target.schema }}` |
| `{% if ... %}...{% endif %}` | Conditional logic — `{% if is_incremental() %}` |
| `{% set x = value %}` | Set a variable |
| `{# comment #}` | Comment (not rendered) |
| `~ variable ~` | String concatenation in expressions |

dbt compiles Jinja first, then runs the resulting SQL against the warehouse. You can see compiled SQL in `dbt/target/compiled/`.

---

## dispatch block in dbt_project.yml

```yaml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['poc_dwh', 'dbt_utils']
```

When dbt sees `dbt_utils.some_macro`, it checks your project (`poc_dwh`) first, then falls back to `dbt_utils`. This lets you override any package macro locally without forking the package.
