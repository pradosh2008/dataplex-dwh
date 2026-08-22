# Phase 1 — First Working Model (HR Domain)

**Goal:** Get one domain running end-to-end. HR only. No Airflow. No other domains.

**Branch:** `phase/1-hr-domain`  
**Status:** complete ✓ — verified 2026-08-22

---

## What gets built

```
dbt/
  packages.yml
  dbt_project.yml
  seeds/
    seed_hr__department_lkp.csv
  models/
    staging/hr/
      sources.yml
      stg_hr__employees.sql
      stg_hr__employees.yml
    intermediate/hr/
      int_hr__employee_metrics.sql
    marts/hr/
      hr__employee_summary.sql
```

---

## Files explained

### `packages.yml`
Declares external dbt packages. Run `dbt deps` once to install them into `dbt/dbt_packages/` (gitignored).
- `dbt_utils` — utility macros (surrogate_key, date helpers etc.)
- `elementary-data` — observability: logs run metadata into the warehouse on every build

### `dbt_project.yml`
Central project config. Key decisions:
- `profile: poc_dwh` → links to `profiles.yml`
- Staging → `view` materialisation → `dwh_udev.staging`
- Intermediate → `ephemeral` → never stored, inlined as CTE into mart
- Marts/hr → `incremental` → `dwh_udev.hr`
- Seeds → `dwh_udev.seeds`
- `on-run-end: elementary.on_run_end()` → auto-logs every run

### `seed_hr__department_lkp.csv`
4-row lookup: `department_id` → `department_name` + `cost_centre`.
Loaded with `dbt seed`. Referenced in the intermediate model to enrich employees with readable dept names.

### `sources.yml` (staging/hr/)
Declares the `hr_raw` source pointing to `dwh_raw.hr`. This is what makes `{{ source('hr_raw', 'raw_hr__employees') }}` work. Kept separate from model YML so all staging models in this domain share one source declaration.

### `stg_hr__employees.sql`
Staging view — renames columns, adds `_loaded_at` timestamp. No business logic.
Reads from: `{{ source('hr_raw', 'raw_hr__employees') }}` → resolves to `dwh_raw.hr.raw_hr__employees`

### `stg_hr__employees.yml`
Model-level tests:
- `employee_id` not_null
- `status` accepted_values: `['active', 'terminated']`

### `int_hr__employee_metrics.sql`
Ephemeral — inlined as CTE when mart runs. Enriches employees with:
- `department_name`, `cost_centre` (left join on seed)
- `tenure_days` (datediff event_date, hire_date)
- `is_active` (1/0 integer flag — sums cleanly)
- `salary_band` (junior / mid / senior)

Left join used intentionally — employees with unknown dept_id still flow through (not silently dropped).

### `hr__employee_summary.sql`
Incremental mart — the actual output table analysts query.

**Materialisation:** `replace_where`
- On nightly run: deletes rows where `event_date > max(event_date) - 2 days`, recomputes and inserts fresh
- Self-healing: late-arriving or corrected source data gets picked up automatically
- On first run (`--full-refresh`): loads all data

**Backfill:** pass `--vars '{"start_of_backfill_window": "YYYY-MM-DD", "end_of_backfill_window": "YYYY-MM-DD"}'` to reprocess a specific date range

**Partitioning:** `event_date`, `site_id` — queries filtering on these scan only relevant partitions  
**Clustering:** `department_name` — sorts within partitions for fast dept-filtered queries

---

## Run commands

```bash
cd dbt

# 1. Install packages (once)
dbt deps

# 2. Load seed table
dbt seed --target udev

# 3. First full run
dbt build --target udev --select tag:hr --full-refresh

# 4. Nightly incremental (simulate)
dbt build --target udev --select tag:hr

# 5. Backfill a date range
dbt build --target udev --select tag:hr \
  --vars '{"start_of_backfill_window": "2026-08-18", "end_of_backfill_window": "2026-08-19"}'
```

## Verify

```sql
SELECT * FROM dwh_udev.hr.hr__employee_summary;
-- expect 5 rows (combinations of event_date / site_id / dept / salary_band)
```

---

## Key concepts introduced in this phase

| Concept | Where |
|---|---|
| `{{ source() }}` vs hardcoded table | `stg_hr__employees.sql` |
| `{{ ref() }}` for model dependencies | `int_hr__employee_metrics.sql` |
| Ephemeral materialisation / CTE inlining | `int_hr__employee_metrics.sql` |
| `replace_where` incremental strategy | `hr__employee_summary.sql` |
| Self-healing 2-day lookback | `hr__employee_summary.sql` |
| `is_incremental()` macro | `hr__employee_summary.sql` |
| `{{ this }}` — current model reference | `hr__employee_summary.sql` |
| Backfill vars pattern | `hr__employee_summary.sql` |
| `sources.yml` separate from model YML | `staging/hr/sources.yml` |

---

---

## Elementary schema

Elementary models land in `dwh_udev.elementary` (30 tables/views — run metadata, test results, model lineage).

**One-time setup:** `dbt run -s elementary --target udev --full-refresh`  
After that, the `on-run-end` hook keeps them updated automatically on every `dbt build`.

If elementary models end up in the wrong schema (e.g. `staging`), the cause is a duplicate `models:` YAML key — the second block silently overwrites the first. Fix: merge both under one `models:` top-level key in `dbt_project.yml`.

---

## Issues encountered and resolved

### 1. Wrong schema names (staging_hr instead of hr)
**Cause:** dbt's default `generate_schema_name` macro prefixes `target.schema` + `custom_schema` → `staging_hr`.  
**Fix:** Added `macros/generate_schema_name.sql` to use the custom schema directly.  
**Learning:** Every production dbt project needs this override. See `docs/dbt-fundamentals.md`.

### 2. Invalid PAT token
**Cause:** Token generated via Databricks UI came in an unexpected format (no `dapi` prefix). The `databricks-sql-connector` rejected it with 401.  
**Fix:** Used the MCP token (`dapifbacdbb...`) which is a valid Databricks PAT.  
**Learning:** Valid Databricks PATs always start with `dapi`. If a generated token doesn't, something went wrong during generation.

### 3. Seed in wrong schema after macro fix
**Cause:** First run (before macro fix) loaded seed into `staging_seeds`. After macro fix, models expected it in `seeds`.  
**Fix:** Re-ran `dbt seed` to reload into correct schema.  
**Learning:** When schema naming changes, re-seed before building. Seed tags are separate from domain tags — run `dbt seed` independently.

### 4. `dbt build --select tag:hr` doesn't include seeds
**Cause:** Seed is tagged `seeds`, not `hr`. The selector skips it.  
**Fix:** Run `dbt seed` separately, then `dbt build --select tag:hr`.  
**Design decision:** Seeds kept separate — they're shared reference data, not domain-specific.

---

## Verified output

```sql
SELECT * FROM dwh_udev.hr.hr__employee_summary;
-- 6 rows
```

| event_date | site_id | department | salary_band | employees | active | avg_salary |
|---|---|---|---|---|---|---|
| 2026-08-18 | 1 | Engineering | mid | 1 | 1 | 75,000 |
| 2026-08-18 | 1 | Sales | mid | 1 | 1 | 65,000 |
| 2026-08-18 | 2 | Engineering | senior | 1 | 0 | 80,000 |
| 2026-08-19 | 1 | Sales | senior | 1 | 1 | 90,000 |
| 2026-08-19 | 2 | Operations | junior | 1 | 1 | 55,000 |
| 2026-08-20 | 3 | Engineering | mid | 1 | 1 | 72,000 |

---

**Next:** [Phase 2 — Add Marketplace Domain](./phase-2-marketplace.md)
