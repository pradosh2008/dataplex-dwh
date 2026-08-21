# Environment Notes

Hard-won findings about the Databricks workspace and local tooling.
Add to this file whenever you discover something non-obvious about the environment.

---

## Databricks Workspace

### Trial workspace types — critical distinction

There are two kinds of Databricks trial and they are not the same:

| Trial type | How provisioned | Cluster options |
|---|---|---|
| **Databricks-hosted trial** (14-day "For Work") | Databricks manages the cloud infra | Serverless SQL Warehouses only — no All-Purpose clusters, no Job clusters, no Classic SQL Warehouses |
| **Cloud-linked trial** | You link your own AWS / GCP / Azure account | Full classic workspace — All-Purpose clusters, Job clusters, Classic SQL Warehouses all available |

**Why it matters for this project:**
- Phases 1–7 (all dbt modeling) work fine on a Databricks-hosted trial — Serverless SQL Warehouse is sufficient.
- Phases 8+ (Airflow + Job clusters) require either a cloud-linked workspace or a local Airflow workaround.

### Unlocking the full workspace

To get All-Purpose and Job clusters, link a cloud account during Databricks signup or workspace creation:
- AWS account → Databricks provisions EC2
- GCP account → Databricks provisions GCE VMs
- Azure account → Databricks provisions VMs

Databricks provides $400 free trial credits which cover all compute for this POC several times over.
Underlying cloud infra (EC2/GCE/VMs) may incur small charges outside Databricks credits.

### Current workspace state (as of 2026-08-22)

| Property | Value |
|---|---|
| Workspace host | `dbc-9924eb44-5d89.cloud.databricks.com` |
| Trial type | Databricks-hosted (serverless only) |
| SQL Warehouse | `dwh-udev` — Serverless, 2X-Small, ID: `39a77203292f5c0b` |
| HTTP path | `/sql/1.0/warehouses/39a77203292f5c0b` |
| All-Purpose clusters | ❌ Not available on this trial |
| Job clusters | ❌ Not available on this trial |

### Workaround for Airflow phases (8+) on serverless trial

Run Airflow locally via Astro CLI (Docker-based). Airflow connects to Databricks via REST API and submits dbt runs against the Serverless SQL Warehouse. The DAG code is identical to what the spec describes — only the compute backing the dbt run differs (Serverless instead of Job cluster). This is production-realistic — many teams run Airflow on Kubernetes and compute on Databricks.

---

## SQL Warehouses

### Serverless vs Classic

| | Serverless | Classic |
|---|---|---|
| Startup time | ~2–5 seconds | ~2–5 minutes |
| Scaling | Instant, managed by Databricks | Manual cluster scaling |
| Cost model | Per query (DBU/second) | Per cluster-hour |
| Available on trial | ✓ | ❌ (cloud-linked only) |
| dbt connection | Same `http_path` format | Same `http_path` format |

dbt does not know or care whether the warehouse is Serverless or Classic — the connection config (`host`, `http_path`, `token`) is identical.

---

## Personal Access Tokens (PAT)

dbt authenticates to Databricks using a PAT. Generate one at:
> Databricks UI → top-right avatar → Settings → Developer → Access tokens → Generate new token

- Name tokens after their purpose: `dbt-udev`, `dbt-upro`
- Store in environment variables (`DBT_TOKEN`), never in committed files
- `dbt/profiles.yml` is gitignored for this reason
