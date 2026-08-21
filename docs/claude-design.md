# Claude + MCP Design — dataplex-dwh

## Overview

This project uses Claude Code (CLI) running on **AWS Bedrock** with MCP for live Databricks SQL access.
State is persisted across sessions via two mechanisms: `CLAUDE.md` (project context) and the memory system.

---

## Model Setup

Configured globally in `~/.claude/settings.json`:

| Setting | Value |
|---|---|
| Default model | `sonnet` (resolves to Claude Sonnet 4.6 via Bedrock ARN) |
| Backend | AWS Bedrock (`CLAUDE_CODE_USE_BEDROCK=1`) |
| AWS Profile | `claude` (auto-refreshed via `awsAuthRefresh`) |
| Region | `eu-west-1` |

Three model tiers are mapped to Bedrock inference profile ARNs — Haiku, Sonnet, Opus — so `claude` CLI commands always resolve to the correct Bedrock endpoint without specifying ARNs manually.

---

## MCP Setup — this project

**One MCP server: `databricks-dataplex`**

| Property | Value |
|---|---|
| Type | HTTP |
| URL | `https://dbc-9924eb44-5d89.cloud.databricks.com/api/2.0/mcp/sql` |
| Auth | Bearer token (PAT) in `Authorization` header |
| Config file | `.claude/settings.local.json` (gitignored — contains token) |

### Why `.claude/settings.local.json` and not `~/.claude.json`

- `settings.local.json` is project-scoped and gitignored — credentials stay local and travel with the project folder
- `~/.claude.json` is where `claude mcp add` writes (internal CLI state) — used by other projects that ran `claude mcp add` interactively, but this project manages MCP config explicitly in files

### settings.local.json structure

```json
{
  "enabledMcpjsonServers": ["databricks-dataplex"],
  "enableAllProjectMcpServers": true,
  "mcpServers": {
    "databricks-dataplex": {
      "type": "http",
      "url": "https://dbc-9924eb44-5d89.cloud.databricks.com/api/2.0/mcp/sql",
      "headers": {
        "Authorization": "Bearer <PAT>"
      }
    }
  }
}
```

`enableAllProjectMcpServers: true` means Claude auto-connects to this MCP on session start without prompting.

---

## Config Layer Hierarchy

Claude Code resolves MCP and settings from multiple sources, in priority order:

| Layer | File | Scope | Notes |
|---|---|---|---|
| 1 | `~/.claude/settings.json` | Global user | Model config, plugins, env vars — committed behavior |
| 2 | `.claude/settings.json` | Project (tracked) | Shareable project settings — no secrets |
| 3 | `.claude/settings.local.json` | Project (gitignored) | Credentials, MCP tokens — never committed |
| 4 | `~/.claude.json` | Global internal state | Written by `claude mcp add`; per-project MCP entries keyed by directory path |

For this project only layers 1 and 3 are populated.

---

## State Persistence Design

### 1. `CLAUDE.md` — session context (committed)

`CLAUDE.md` at project root is always injected into every Claude session automatically. It carries:
- What the project is and what phase it's at
- Three-catalog architecture decision
- Naming conventions
- dbt patterns (source YML, incremental strategy)
- Phase roadmap (quick reference)
- Key file pointers

This is the single source of truth Claude reads to orient itself in a new session. It must be kept current as the project evolves.

**Rule:** after each phase completes, update `CLAUDE.md` to reflect the new current state.

### 2. Memory system — cross-session facts

Stored at `~/.claude/projects/-Users-pradosh-jena-projects-dataplex-dwh/memory/`.
Indexed via `MEMORY.md` in the same directory.

Four memory types:

| Type | What it captures |
|---|---|
| `user` | Who is working here, their background, preferences |
| `feedback` | Corrections and confirmed approaches — avoids repeating mistakes |
| `project` | Decisions, motivations, deadlines not in the code |
| `reference` | Where to find things in external systems |

Memory persists across sessions; `CLAUDE.md` does not need to duplicate it.

### 3. What is NOT persisted

- Git history, file structure, architecture — Claude reads these live
- In-session task lists and plans — live in the conversation only
- Temporary debugging notes — belong in commit messages, not memory

---

## MCP Usage Pattern

The `databricks-dataplex` MCP exposes `execute_sql` and `execute_sql_read_only` tools.

**Read-only queries** (checking schema, previewing data, validating setup):
→ use `execute_sql_read_only`

**Write/DDL** (only for ad-hoc setup work, never during dbt runs):
→ use `execute_sql`

dbt itself connects to Databricks via `profiles.yml` (not via MCP) — MCP is for Claude to inspect and validate the warehouse during development, not to run dbt pipelines.

---

## What is gitignored

```
.claude/settings.local.json   # MCP token
dbt/profiles.yml              # Databricks connection tokens for dbt
dbt/target/
dbt/dbt_packages/
dbt/logs/
.env
```
