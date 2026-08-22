# Project Git History

Complete record of every commit and PR merged into `main`, in chronological order.
Updated manually as new phases land.

---

## 2026-08-21 — Project kickoff

### `b9665a5` — chore: initial project setup
**Branch:** direct to main  
Base `README.md` (full project overview, folder structure, phase roadmap) and `.gitignore` (profiles.yml, dbt targets, .env, pycache).

---

## 2026-08-21 — Phase 0: Prerequisites

### PR #1 — `phase/0-prerequisites`
Merged: 2026-08-21

**`1d2cbd8`** — feat: phase 0 — prerequisites, folder structure, setup SQL  
Everything needed before dbt can run:
- Full `dbt/` folder structure (staging, intermediate, marts, analytics, reporting, dwh_internal, macros, seeds, snapshots)
- `airflow/src/dwh/` directory
- `sql/phase_0_setup.sql` — one-time Databricks SQL to create all three catalogs (`dwh_raw`, `dwh_udev`, `dwh_upro`) and all schemas
- `dbt/profiles.yml.example` — template with four targets (udev, udev_serverless, upro, upro_serverless)
- Phase 0 doc at `docs/phases/phase-0-prerequisites.md`
- Full spec at `docs/spec.md`

---

## 2026-08-21 — Fix: Raw Catalog Separation

### PR #2 + PR #3 — `fix/phase-0-raw-catalog-separation`
Merged: 2026-08-21 (two PRs — fix landed in two rounds)

**`93d14db`** — fix: separate raw landing zone into dwh_raw catalog  
Key architectural correction: raw tables must not live in `dwh_udev.staging`. Moved to a dedicated `dwh_raw` catalog with domain schemas (`hr`, `mkt`, `pay`). Updated `sql/phase_0_setup.sql` to create `dwh_raw` and its schemas. Updated `docs/spec.md` source YML blocks and `dbt/profiles.yml.example`.

**`006bee7`** — chore: add adhoc_cleanup.sql to drop old raw tables from dwh_udev.staging  
`sql/adhoc_cleanup.sql` — one-time cleanup script to drop the incorrectly placed raw tables from `dwh_udev.staging`. Already run against the workspace.

**Databricks state after this fix:**
- `dwh_raw.hr.raw_hr__employees` ✓
- `dwh_raw.mkt.raw_mkt__listings` ✓
- `dwh_raw.pay.raw_pay__transactions` ✓

---

## 2026-08-22 — Project Tooling & Doc Corrections

### PR #4 — `chore/project-setup-and-docs`
Merged: 2026-08-22

**`2f534b3`** — chore: project setup, tooling, and doc corrections

New files:
- `CLAUDE.md` — project context injected into every Claude Code session (architecture, naming conventions, phase roadmap, dbt patterns)
- `databricks.yml` — Databricks Asset Bundle definition with `dev` (dwh_udev) and `prod` (dwh_upro) targets; placeholder hosts for future workspace provisioning
- `docs/claude-design.md` — documents Claude + MCP setup: model config (Bedrock ARNs), MCP server inventory, config layer hierarchy, state persistence design
- `.github/workflows/.gitkeep` — placeholder to track the CI/CD directory ahead of Phase 15

Doc corrections (follow-on to raw catalog fix):
- `docs/spec.md` — fixed all 3 source YAML blocks (hr, mkt, pay) from `database: "{{ target.catalog }}" / schema: staging` to `database: dwh_raw / schema: <domain>`; added `databricks.yml` explanation under Phase 15
- `docs/phases/phase-0-prerequisites.md` — updated naming convention table and Phase 0 completion checklist to reference `dwh_raw` correctly

Housekeeping:
- `.gitignore` — added `.claude/settings.local.json` and `.mcp.json`
- `README.md` — corrected sql/ filename `adhoc_sql.sql` → `adhoc_cleanup.sql`

---

## Phase 0 complete — Databricks state verified 2026-08-22

```
dwh_raw
  hr    → raw_hr__employees
  mkt   → raw_mkt__listings
  pay   → raw_pay__transactions

dwh_udev
  staging, intermediate, hr, marketplace, payments,
  analytics, reporting, dwh_internal, seeds

dwh_upro  (same schema structure as dwh_udev)
```

---

## 2026-08-22 — Phase 1: HR Domain (in progress)

### Branch: `phase/1-hr-domain`
Status: models built, not yet run against warehouse

**Files added:**
- `dbt/packages.yml` — declares `dbt_utils` and `elementary-data` dependencies
- `dbt/dbt_project.yml` — project config: profile binding, folder paths, per-layer materialisation defaults, `on-run-end` elementary hook
- `dbt/seeds/seed_hr__department_lkp.csv` — 4-row dept lookup (id → name + cost centre)
- `dbt/models/staging/hr/sources.yml` — declares `hr_raw` source pointing to `dwh_raw.hr`
- `dbt/models/staging/hr/stg_hr__employees.sql` — staging view: renames columns, adds `_loaded_at`
- `dbt/models/staging/hr/stg_hr__employees.yml` — model description + `not_null` / `accepted_values` tests
- `dbt/models/intermediate/hr/int_hr__employee_metrics.sql` — ephemeral: joins dept seed, calculates `tenure_days`, `is_active`, `salary_band`
- `dbt/models/marts/hr/hr__employee_summary.sql` — incremental mart: `replace_where` strategy, 2-day self-healing lookback, backfill vars, partitioned by `event_date`/`site_id`, clustered by `department_name`
- `docs/environment.md` — Databricks trial constraints, workspace state, serverless vs classic, PAT guidance
- `docs/git-history.md` — this file

**Key decisions made:**
- Serverless SQL warehouse (`dwh-udev`) used throughout due to Databricks trial constraint (no classic clusters available)
- `sources.yml` kept separate from model YML — one per domain folder, shared by all staging models in that domain
- Intermediate model kept ephemeral — pure enrichment logic, never queried directly
- `macros/generate_schema_name.sql` added — overrides dbt's default schema prefixing behaviour so models land in clean schemas (`hr`, `staging`) instead of `staging_hr`, `staging_staging`
- PAT for dbt: `dapifbacdbb...` (same as MCP token) — the token generated via Databricks UI came in an invalid format (non-`dapi` prefix); MCP token works for both

**Verified output (2026-08-22):**
- `dwh_udev.staging.stg_hr__employees` — view ✓
- `dwh_udev.seeds.seed_hr__department_lkp` — 4 rows ✓
- `dwh_udev.hr.hr__employee_summary` — 6 rows ✓

---

## Upcoming

| Phase | Branch (planned) | Status |
|---|---|---|
| 1 — HR domain, first model | `phase/1-hr-domain` | complete — pending merge |
| 2 — Marketplace domain | `phase/2-marketplace` | pending |
| 3 — Payments domain | `phase/3-payments` | pending |
| 4–17 | see `docs/spec.md` | pending |
