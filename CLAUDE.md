# dataplex-dwh — Claude context

## What this project is
A 17-phase incremental build of a Databricks DWH using dbt + Airflow.
Full spec: `docs/spec.md`. Phase docs: `docs/phases/`.

## Current state
- **Phase 0 complete** — catalogs, schemas, raw tables, folder structure all done
- **Active branch:** `fix/phase-0-raw-catalog-separation` — not yet merged to main
- **Databricks MCP:** configured at `.claude/settings.local.json` (gitignored), server name `databricks-dataplex`, workspace `dbc-9924eb44-5d89.cloud.databricks.com`
- **Next action:** merge the fix branch to main, then start Phase 1 (HR domain only)

## Three-catalog architecture (key decision)
| Catalog | Role | dbt interaction |
|---|---|---|
| `dwh_raw` | Bronze — raw landing zone | reads only (never writes) |
| `dwh_udev` | Silver + Gold (dev) | writes all output |
| `dwh_upro` | Silver + Gold (prod) | writes all output |

Raw tables are NOT in `dwh_udev.staging` — they live in `dwh_raw` with domain schemas:
- `dwh_raw.hr` — HR source tables
- `dwh_raw.mkt` — Marketplace source tables
- `dwh_raw.pay` — Payments source tables

## dbt source YML pattern
All source blocks must use `database: dwh_raw` + the domain schema, not `target.catalog`:
```yaml
sources:
  - name: hr_raw
    database: dwh_raw
    schema: hr
```

## dbt targets (profiles.yml — not committed)
| Target | Catalog | Compute | When used |
|---|---|---|---|
| `udev` | dwh_udev | classic warehouse | default local dev |
| `udev_serverless` | dwh_udev | serverless warehouse | added Phase 9 |
| `upro` | dwh_upro | classic warehouse | added Phase 9 |
| `upro_serverless` | dwh_upro | serverless warehouse | added Phase 9 |

## Naming conventions
- `raw_<domain>__<entity>` — raw table in `dwh_raw.<domain>`
- `stg_<domain>__<entity>` — dbt staging view
- `int_<domain>__<entity>_<suffix>` — dbt intermediate (ephemeral)
- `<domain>__<entity>` — dbt mart (incremental table)
- `perf__<entity>` — analytics layer
- `exec__<entity>` — reporting layer
- `seed_<domain>__<entity>_lkp` — dbt seed CSV

Double underscore (`__`) separates domain prefix from entity name.

## Incremental pattern used in all mart models
`replace_where` strategy with a self-healing 2-day lookback window.
Backfill vars: `start_of_backfill_window` / `end_of_backfill_window`.

## Phase roadmap (quick reference)
```
✅  0   Prerequisites — catalogs, schemas, raw data
⬜  1   First model — HR domain only, no Airflow
⬜  2   Add marketplace domain
⬜  3   Add payments domain
⬜  4   Analytics layer (cross-domain)
⬜  5   Reporting layer
⬜  6   Macros
⬜  7   Selectors
⬜  8   Airflow v1 — dbt_daily only
⬜  9   Airflow v2 — HR as serverless group
⬜ 10   Airflow v3 — marketplace as serverless group
⬜ 11   Airflow v4 — analytics_layer as serverless group
⬜ 12   Airflow v5 — payments dual-run (AP + serverless shadow)
⬜ 13   Airflow v6 — elementary alert tests
⬜ 14   DWH internal validation models
⬜ 15   CI/CD pipeline
⬜ 16   Cut payments to serverless-only
⬜ 17   Cost monitoring dashboard
```

## Key files
- `docs/spec.md` — full phase-by-phase build guide
- `sql/phase_0_setup.sql` — one-time Databricks setup SQL (already run)
- `sql/adhoc_cleanup.sql` — drops old raw tables from dwh_udev.staging (already run)
- `dbt/profiles.yml.example` — copy to profiles.yml and fill in tokens
- `databricks.yml` — bundle definition (dev/prod targets, host placeholders)
