# Build Guide: BNL-Style Databricks DWH — Incremental Build

**Philosophy:** Every file starts minimal. You add one concept at a time, run it, verify it works,
then extend. By the end you have the full project — and you've understood every piece of it.

**What you'll build across 17 phases:**

```
Phase 0   Prerequisites — Databricks setup, folder, git
Phase 1   First working model — HR domain only, no Airflow
Phase 2   Add marketplace domain
Phase 3   Add payments domain
Phase 4   Add analytics layer (cross-domain)
Phase 5   Add reporting layer
Phase 6   Add macros
Phase 7   Add selectors
Phase 8   Airflow v1 — dbt_daily only (AP cluster, no domain groups yet)
Phase 9   Airflow v2 — add HR as a serverless group
Phase 10  Airflow v3 — add marketplace as a serverless group
Phase 11  Airflow v4 — add analytics_layer as a serverless group
Phase 12  Airflow v5 — add payments as a dual-run group (AP + serverless shadow)
Phase 13  Airflow v6 — wire in elementary alert tests
Phase 14  Add DWH internal validation models
Phase 15  CI/CD pipeline
Phase 16  Cut over payments to serverless-only
Phase 17  Cost monitoring dashboard — understand what everything costs
```

---

## Phase 0 — Prerequisites

### 0.1 Install tools

```bash
python --version        # 3.11+ required
pip install dbt-databricks==1.8.7
pip install elementary-data==0.14.3
pip install databricks-cli
```

### 0.2 Create project folder

```bash
mkdir poc-dwh
cd poc-dwh
git init
```

Create `.gitignore`:
```
dbt/profiles.yml
dbt/target/
dbt/dbt_packages/
dbt/logs/
.env
__pycache__/
*.pyc
```

### 0.3 Create folder structure

```bash
mkdir -p dbt/models/staging/{hr,marketplace,payments}
mkdir -p dbt/models/intermediate/{hr,marketplace,payments}
mkdir -p dbt/models/marts/{hr,marketplace,payments}
mkdir -p dbt/models/analytics/performance
mkdir -p dbt/models/reporting/executive
mkdir -p dbt/models/dwh_internal
mkdir -p dbt/macros/{validation,utils}
mkdir -p dbt/seeds
mkdir -p dbt/snapshots
mkdir -p airflow/src/dwh
mkdir -p sql
mkdir -p .github/workflows
```

### 0.4 Databricks — create catalogs and schemas

Run in Databricks SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS dwh_udev;
CREATE CATALOG IF NOT EXISTS dwh_upro;

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

USE CATALOG dwh_upro;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS marketplace;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS reporting;
CREATE SCHEMA IF NOT EXISTS dwh_internal;
CREATE SCHEMA IF NOT EXISTS seeds;
```

### 0.5 Databricks — create raw source tables

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

**Verify:**
```sql
SELECT COUNT(*) FROM dwh_udev.staging.raw_hr__employees;      -- 6
SELECT COUNT(*) FROM dwh_udev.staging.raw_mkt__listings;      -- 5
SELECT COUNT(*) FROM dwh_udev.staging.raw_pay__transactions;  -- 4
```

---

## Phase 1 — First Working Model (HR only)

Goal: get one domain running end-to-end. HR only. No other domains. No Airflow yet.

### 1.1 `dbt/packages.yml`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: elementary-data/elementary
    version: [">=0.14.0", "<1.0.0"]
```

### 1.2 `dbt/profiles.yml` (not committed)

```yaml
poc_dwh:
  target: udev
  outputs:
    udev:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: "{{ env_var('DBT_TOKEN') }}"
      catalog: dwh_udev
      schema: staging
      threads: 8
```

We'll add the other three targets (`udev_serverless`, `upro`, `upro_serverless`) in Phase 9
when we need them. No point adding them now.

### 1.3 `dbt/dbt_project.yml` — HR only

Start minimal. One domain, one schema per layer.

```yaml
name: poc_dwh
version: '1.0.0'
config-version: 2
profile: poc_dwh

model-paths: ["models"]
seed-paths:  ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

dispatch:
  - macro_namespace: dbt_utils
    search_order: ['poc_dwh', 'dbt_utils']

models:
  poc_dwh:

    staging:
      +schema: staging
      +materialized: view
      hr:
        +tags: ['hr']

    intermediate:
      +schema: intermediate
      +materialized: ephemeral
      hr:
        +tags: ['hr']

    marts:
      hr:
        +schema: hr
        +materialized: incremental
        +tags: ['hr']

seeds:
  poc_dwh:
    +schema: seeds
    +tags: ['seeds']

on-run-end:
  - "{{ elementary.on_run_end() }}"
```

### 1.4 HR seed

`dbt/seeds/seed_hr__department_lkp.csv`:
```csv
department_id,department_name,cost_centre
10,Engineering,CC-ENG
20,Sales,CC-SALES
30,Operations,CC-OPS
40,Finance,CC-FIN
```

### 1.5 HR staging

`dbt/models/staging/hr/stg_hr__employees.sql`:
```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('hr_raw', 'raw_hr__employees') }}
),
renamed as (
    select
        employee_id,
        site_id,
        department_id,
        full_name,
        status,
        salary,
        hire_date,
        event_date,
        current_timestamp() as _loaded_at
    from source
)
select * from renamed
```

`dbt/models/staging/hr/stg_hr__employees.yml`:
```yaml
version: 2

sources:
  - name: hr_raw
    database: dwh_raw
    schema: hr
    tables:
      - name: raw_hr__employees
        columns:
          - name: employee_id
            data_tests: [not_null]
          - name: event_date
            data_tests: [not_null]

models:
  - name: stg_hr__employees
    description: "Renamed HR employee records"
    columns:
      - name: employee_id
        data_tests: [not_null]
      - name: status
        data_tests:
          - accepted_values:
              values: ['active', 'terminated']
```

### 1.6 HR intermediate

`dbt/models/intermediate/hr/int_hr__employee_metrics.sql`:
```sql
{{ config(materialized='ephemeral') }}

with employees as (
    select * from {{ ref('stg_hr__employees') }}
),
departments as (
    select * from {{ ref('seed_hr__department_lkp') }}
),
enriched as (
    select
        e.employee_id,
        e.site_id,
        e.full_name,
        e.status,
        e.salary,
        e.hire_date,
        e.event_date,
        d.department_name,
        d.cost_centre,
        datediff(e.event_date, e.hire_date)              as tenure_days,
        case when e.status = 'active' then 1 else 0 end  as is_active,
        case
            when e.salary < 60000 then 'junior'
            when e.salary < 80000 then 'mid'
            else 'senior'
        end                                              as salary_band
    from employees as e
    left join departments as d on e.department_id = d.department_id
)
select * from enriched
```

### 1.7 HR mart

`dbt/models/marts/hr/hr__employee_summary.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=[
            (
                "event_date between '"
                ~ var("start_of_backfill_window")
                ~ "' and '"
                ~ var("end_of_backfill_window")
                ~ "'"
                if var("start_of_backfill_window", false) and var("end_of_backfill_window", false)
                else "event_date > date_sub((select max(event_date) from " ~ this ~ "), 2)"
            )
        ],
        partition_by=['event_date', 'site_id'],
        cluster_by=['department_name'],
        tags=['hr']
    )
}}

with employee_metrics as (
    select * from {{ ref('int_hr__employee_metrics') }}
    {% if var("start_of_backfill_window", false) and var("end_of_backfill_window", false) %}
        where event_date between '{{ var("start_of_backfill_window") }}' and '{{ var("end_of_backfill_window") }}'
    {% else %}
        {% if is_incremental() %}
            where event_date > date_sub((select max(event_date) from {{ this }}), 2)
        {% endif %}
    {% endif %}
),
aggregated as (
    select
        event_date,
        site_id,
        department_name,
        cost_centre,
        salary_band,
        count(employee_id)      as employee_count,
        sum(is_active)          as active_employee_count,
        avg(salary)             as avg_salary,
        avg(tenure_days)        as avg_tenure_days
    from employee_metrics
    group by 1, 2, 3, 4, 5
)
select * from aggregated
```

### 1.8 Run it

```bash
cd dbt
export DBT_TOKEN=<your-databricks-token>
dbt deps                                              # install packages
dbt seed --target udev                                # load HR lookup table
dbt build --target udev --select tag:hr --full-refresh  # staging + intermediate + mart
```

**Verify:**
```sql
SELECT * FROM dwh_udev.hr.hr__employee_summary;
-- 5 rows (one per event_date/site_id/department/salary_band combination)
```

**Run incremental (simulate nightly):**
```bash
dbt build --target udev --select tag:hr
# Only reprocesses partitions from max(event_date) - 2 onwards
```

**Run backfill:**
```bash
dbt build --target udev --select tag:hr \
  --vars '{"start_of_backfill_window": "2026-08-18", "end_of_backfill_window": "2026-08-19"}'
# Only replaces 2026-08-18 and 2026-08-19 partitions
```

---

## Phase 2 — Add Marketplace Domain

Add a second domain. You'll see the pattern from Phase 1 repeats exactly.

### 2.1 Update `dbt_project.yml` — add marketplace section

Add these sections alongside the existing ones:

```yaml
# Under models > poc_dwh > staging:
      marketplace:
        +tags: ['marketplace']

# Under models > poc_dwh > intermediate:
      marketplace:
        +tags: ['marketplace']

# Under models > poc_dwh > marts:
      marketplace:
        +schema: marketplace
        +materialized: incremental
        +tags: ['marketplace']
```

### 2.2 Marketplace seed

`dbt/seeds/seed_mkt__category_lkp.csv`:
```csv
category_id,category_name,parent_category
10,Electronics,Tech
20,Clothing,Fashion
30,Home & Garden,Home
40,Motors,Vehicles
```

### 2.3 Marketplace staging

`dbt/models/staging/marketplace/stg_mkt__listings.sql`:
```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('mkt_raw', 'raw_mkt__listings') }}
),
renamed as (
    select
        listing_id,
        seller_id,
        category_id,
        site_id,
        price,
        status,
        listed_date,
        event_date,
        current_timestamp() as _loaded_at
    from source
)
select * from renamed
```

`dbt/models/staging/marketplace/stg_mkt__listings.yml`:
```yaml
version: 2

sources:
  - name: mkt_raw
    database: dwh_raw
    schema: mkt
    tables:
      - name: raw_mkt__listings

models:
  - name: stg_mkt__listings
    description: "Renamed marketplace listings"
    columns:
      - name: listing_id
        data_tests: [not_null]
      - name: status
        data_tests:
          - accepted_values:
              values: ['active', 'sold', 'expired']
```

### 2.4 Marketplace intermediate

`dbt/models/intermediate/marketplace/int_mkt__listing_metrics.sql`:
```sql
{{ config(materialized='ephemeral') }}

with listings as (
    select * from {{ ref('stg_mkt__listings') }}
),
categories as (
    select * from {{ ref('seed_mkt__category_lkp') }}
),
enriched as (
    select
        l.listing_id,
        l.seller_id,
        l.site_id,
        l.price,
        l.status,
        l.listed_date,
        l.event_date,
        c.category_name,
        c.parent_category,
        case when l.status = 'active' then 1 else 0 end  as is_active,
        case when l.status = 'sold'   then 1 else 0 end  as is_sold,
        case
            when l.price < 50  then 'low'
            when l.price < 200 then 'mid'
            else 'high'
        end                                               as price_band,
        datediff(l.event_date, l.listed_date)             as days_listed
    from listings as l
    left join categories as c on l.category_id = c.category_id
)
select * from enriched
```

### 2.5 Marketplace mart

`dbt/models/marts/marketplace/mkt__listing_summary.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=[
            (
                "event_date between '"
                ~ var("start_of_backfill_window")
                ~ "' and '"
                ~ var("end_of_backfill_window")
                ~ "'"
                if var("start_of_backfill_window", false) and var("end_of_backfill_window", false)
                else "event_date > date_sub((select max(event_date) from " ~ this ~ "), 2)"
            )
        ],
        partition_by=['event_date', 'site_id'],
        cluster_by=['category_name'],
        tags=['marketplace']
    )
}}

with listing_metrics as (
    select * from {{ ref('int_mkt__listing_metrics') }}
    {% if var("start_of_backfill_window", false) and var("end_of_backfill_window", false) %}
        where event_date between '{{ var("start_of_backfill_window") }}' and '{{ var("end_of_backfill_window") }}'
    {% else %}
        {% if is_incremental() %}
            where event_date > date_sub((select max(event_date) from {{ this }}), 2)
        {% endif %}
    {% endif %}
),
aggregated as (
    select
        event_date,
        site_id,
        category_name,
        parent_category,
        price_band,
        count(listing_id)   as listing_count,
        sum(is_active)      as active_listing_count,
        sum(is_sold)        as sold_listing_count,
        avg(price)          as avg_price,
        avg(days_listed)    as avg_days_listed
    from listing_metrics
    group by 1, 2, 3, 4, 5
)
select * from aggregated
```

### 2.6 Run it

```bash
dbt seed --target udev                                          # loads the new category seed
dbt build --target udev --select tag:marketplace --full-refresh
```

**Verify:**
```sql
SELECT * FROM dwh_udev.marketplace.mkt__listing_summary;
```

---

## Phase 3 — Add Payments Domain

Payments follows the exact same pattern as HR and marketplace. The only difference is
this domain will be in the dual-run later, so we're setting it up now.

### 3.1 Update `dbt_project.yml` — add payments section

```yaml
# Under staging:
      payments:
        +tags: ['payments']

# Under intermediate:
      payments:
        +tags: ['payments']

# Under marts:
      payments:
        +schema: payments
        +materialized: incremental
        +tags: ['payments']
```

### 3.2 Payments seed

`dbt/seeds/seed_pay__status_lkp.csv`:
```csv
status_code,status_label,is_successful
completed,Payment Completed,true
failed,Payment Failed,false
refunded,Payment Refunded,false
pending,Payment Pending,false
```

### 3.3 Payments staging

`dbt/models/staging/payments/stg_pay__transactions.sql`:
```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('pay_raw', 'raw_pay__transactions') }}
),
renamed as (
    select
        transaction_id,
        buyer_id,
        seller_id,
        listing_id,
        amount,
        currency_code,
        status,
        site_id,
        event_date,
        current_timestamp() as _loaded_at
    from source
)
select * from renamed
```

`dbt/models/staging/payments/stg_pay__transactions.yml`:
```yaml
version: 2

sources:
  - name: pay_raw
    database: dwh_raw
    schema: pay
    tables:
      - name: raw_pay__transactions

models:
  - name: stg_pay__transactions
    description: "Renamed payment transactions"
    columns:
      - name: transaction_id
        data_tests: [not_null]
```

### 3.4 Payments intermediate

`dbt/models/intermediate/payments/int_pay__transaction_metrics.sql`:
```sql
{{ config(materialized='ephemeral') }}

with transactions as (
    select * from {{ ref('stg_pay__transactions') }}
),
statuses as (
    select * from {{ ref('seed_pay__status_lkp') }}
),
enriched as (
    select
        t.transaction_id,
        t.buyer_id,
        t.seller_id,
        t.listing_id,
        t.amount,
        t.currency_code,
        t.status,
        t.site_id,
        t.event_date,
        s.status_label,
        s.is_successful,
        case
            when s.is_successful = 'true' then t.amount
            else 0
        end as successful_amount
    from transactions as t
    left join statuses as s on t.status = s.status_code
)
select * from enriched
```

### 3.5 Payments mart

`dbt/models/marts/payments/pay__transaction_summary.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='replace_where',
        incremental_predicates=[
            (
                "event_date between '"
                ~ var("start_of_backfill_window")
                ~ "' and '"
                ~ var("end_of_backfill_window")
                ~ "'"
                if var("start_of_backfill_window", false) and var("end_of_backfill_window", false)
                else "event_date > date_sub((select max(event_date) from " ~ this ~ "), 2)"
            )
        ],
        partition_by=['event_date', 'site_id'],
        cluster_by=['currency_code'],
        tags=['payments']
    )
}}

with transaction_metrics as (
    select * from {{ ref('int_pay__transaction_metrics') }}
    {% if var("start_of_backfill_window", false) and var("end_of_backfill_window", false) %}
        where event_date between '{{ var("start_of_backfill_window") }}' and '{{ var("end_of_backfill_window") }}'
    {% else %}
        {% if is_incremental() %}
            where event_date > date_sub((select max(event_date) from {{ this }}), 2)
        {% endif %}
    {% endif %}
),
aggregated as (
    select
        event_date,
        site_id,
        currency_code,
        count(transaction_id)               as transaction_count,
        sum(cast(is_successful as int))     as successful_transaction_count,
        sum(successful_amount)              as total_successful_amount,
        avg(amount)                         as avg_transaction_amount
    from transaction_metrics
    group by 1, 2, 3
)
select * from aggregated
```

### 3.6 Run it

```bash
dbt seed --target udev
dbt build --target udev --select tag:payments --full-refresh
```

**Verify:**
```sql
SELECT * FROM dwh_udev.payments.pay__transaction_summary;
```

---

## Phase 4 — Add Analytics Layer

The analytics layer is the first time models read from MORE THAN ONE domain.
`perf__fact_daily` joins all three mart outputs.

### 4.1 Update `dbt_project.yml` — add analytics section

```yaml
# Add under models > poc_dwh:
    analytics:
      +schema: analytics
      +materialized: incremental
      performance:
        +tags: ['analytics']
```

### 4.2 Dimension table

`dbt/models/analytics/performance/perf__dim_category.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='dim_category_key',
        tags=['analytics']
    )
}}

with categories as (
    select * from {{ ref('seed_mkt__category_lkp') }}
),
with_key as (
    select
        {{ dbt_utils.generate_surrogate_key(['category_id']) }}  as dim_category_key,
        category_id,
        category_name,
        parent_category,
        current_timestamp()                                       as _updated_at
    from categories
)
select * from with_key
```

### 4.3 Cross-domain fact table

`dbt/models/analytics/performance/perf__fact_daily.sql`:
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['event_date', 'site_id'],
        tags=['analytics']
    )
}}

with listings as (
    select * from {{ ref('mkt__listing_summary') }}
    {% if is_incremental() %}
        where event_date > date_sub((select max(event_date) from {{ this }}), 2)
    {% endif %}
),
payments as (
    select * from {{ ref('pay__transaction_summary') }}
    {% if is_incremental() %}
        where event_date > date_sub((select max(event_date) from {{ this }}), 2)
    {% endif %}
),
employees as (
    select * from {{ ref('hr__employee_summary') }}
    {% if is_incremental() %}
        where event_date > date_sub((select max(event_date) from {{ this }}), 2)
    {% endif %}
),
joined as (
    select
        l.event_date,
        l.site_id,
        l.category_name,
        l.listing_count,
        l.active_listing_count,
        l.avg_price,
        coalesce(p.transaction_count,            0)  as transaction_count,
        coalesce(p.successful_transaction_count, 0)  as successful_transaction_count,
        coalesce(p.total_successful_amount,      0)  as total_gmv,
        coalesce(e.active_employee_count,        0)  as active_employee_count,
        case
            when l.listing_count > 0
            then coalesce(p.transaction_count, 0) / l.listing_count
            else 0
        end                                          as conversion_rate,
        case
            when coalesce(e.active_employee_count, 0) > 0
            then coalesce(p.total_successful_amount, 0) / e.active_employee_count
            else 0
        end                                          as revenue_per_employee
    from listings  as l
    left join payments  as p on l.event_date = p.event_date and l.site_id = p.site_id
    left join employees as e on l.event_date = e.event_date and l.site_id = e.site_id
)
select * from joined
```

### 4.4 Run it

```bash
dbt build --target udev --select tag:analytics --full-refresh
```

**Verify:**
```sql
SELECT * FROM dwh_udev.analytics.perf__fact_daily;
-- You should see conversion_rate and revenue_per_employee populated
```

---

## Phase 5 — Add Reporting Layer

The reporting layer shapes analytics output for BI consumers.

### 5.1 Update `dbt_project.yml` — add reporting section

```yaml
# Add under models > poc_dwh:
    reporting:
      +schema: reporting
      executive:
        +materialized: table
        +tags: ['reporting']
```

### 5.2 Reporting model

`dbt/models/reporting/executive/exec__report_daily_kpis.sql`:
```sql
{{ config(materialized='table', tags=['reporting']) }}

with daily as (
    select * from {{ ref('perf__fact_daily') }}
    where event_date >= date_sub(current_date(), 90)
),
formatted as (
    select
        event_date                          as Date,
        site_id                             as SiteID,
        category_name                       as Category,
        listing_count                       as TotalListings,
        active_listing_count                as ActiveListings,
        avg_price                           as AvgListingPrice,
        transaction_count                   as Transactions,
        successful_transaction_count        as SuccessfulTransactions,
        total_gmv                           as GrossMarketplaceValue,
        active_employee_count               as ActiveEmployees,
        round(conversion_rate * 100, 2)     as ConversionRatePct,
        round(revenue_per_employee, 2)      as RevenuePerEmployee
    from daily
)
select * from formatted
order by Date desc, GrossMarketplaceValue desc
```

### 5.3 Run it

```bash
dbt build --target udev --select tag:reporting
```

**Verify:**
```sql
SELECT * FROM dwh_udev.reporting.exec__report_daily_kpis;
```

---

## Phase 6 — Add Macros

### 6.1 `is_not_production`

`dbt/macros/utils/is_not_production.sql`:
```sql
{% macro is_not_production() %}
    {{ target.name not in ['upro', 'upro_serverless'] }}
{% endmacro %}
```

You can use this in any model to apply tighter filters when running locally:
```sql
{% if is_not_production() and is_incremental() %}
    and event_date >= date_sub((select max(event_date) from {{ this }}), 1)
{% endif %}
```

### 6.2 `get_serverless_alias` — start empty

We need this macro to exist now, but we'll add the payments paths in Phase 12.
For now it just returns the model name unchanged.

`dbt/macros/validation/get_serverless_alias.sql`:
```sql
{#
    Alias macro for dual-run shadow tables.
    Models listed below get a _serverless suffix when run against a _serverless target.

    Currently in dual-run:
    (none yet — payments will be added in Phase 12)
#}
{% macro generate_alias_name(custom_alias_name="", node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node is not none -%}
        {{ node.name }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
```

**Verify macros compile:**
```bash
dbt compile --target udev --select hr__employee_summary
# No errors = macros are valid
```

---

## Phase 7 — Add Selectors

Selectors are named model selection expressions. Each Airflow task group will use one.
We'll build selectors incrementally, adding each as we add the corresponding Airflow group.

### 7.1 `dbt/selectors.yml` — start with just `daily`

```yaml
selectors:

  - name: daily
    description: "Main daily run — excludes all dedicated task groups"
    definition:
      union:
        - method: path
          value: models/
        exclude:
          - method: tag
            value: elementary_alert
          - method: tag
            value: inactive
          - method: resource_type
            value: snapshot
          # Domain tags will be added here as we add task groups in Phases 9-12
```

**Verify:**
```bash
dbt ls --target udev --selector daily
# Should list ALL models (we haven't excluded any domains yet)
```

---

## Phase 8 — Airflow v1: `dbt_daily` Only

Write the minimal version of the DAG first. Just the `dbt_daily` task group.
No serverless groups, no dual-run, no elementary. That comes in later phases.

### 8.1 `airflow/src/dwh/template_lib.py`

```python
import os
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

PROJECT_NAME = "poc"
DATABRICKS_CONNECTION_ID = "databricks_default"


class EnvHelper:
    def get_environment(self) -> str:
        return os.getenv("DBT_ENVIRONMENT", "udev")

    def on_production(self) -> bool:
        return self.get_environment() == "upro"

    def on_failure_callback(self):
        return lambda ctx: print(f"Task failed: {ctx.get('task_instance')}")


class AssetBundle:
    def get_dbt_project_path(self) -> str:
        return "/Workspace/Repos/poc-dwh/dbt"


env = EnvHelper()
asset_bundle = AssetBundle()


def on_failure_callback():
    return lambda ctx: print(f"Task failed: {ctx.get('task_instance')}")
```

### 8.2 `airflow/src/dwh/project_lib.py` — minimal

```python
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
import json

DATABRICKS_CONNECTION_ID = "databricks_default"
DBT_PROJECT_PATH = "/Workspace/Repos/poc-dwh/dbt"
SQL_WAREHOUSE_ID = "<your-sql-warehouse-id>"

CLUSTER_CONFIG = {
    "spark_version": "15.4.x-scala2.12",
    "node_type_id": "Standard_DS3_v2",
    "num_workers": 2,
}

LIBRARIES = [
    {"pypi": {"package": "dbt-databricks>=1.8.0,<2.0.0"}},
    {"pypi": {"package": "elementary-data>=0.14.0,<1.0.0"}},
]

COMMON_DAG_ARGS = {
    "catchup": False,
    "max_active_runs": 1,
    "default_args": {
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    "tags": ["dbt", "databricks"],
}


def generate_job_config(name, dbt_args, dbt_vars, target,
                        compute_type="new_cluster", use_new_dbt=False):
    commands = [
        f"dbt debug --target={target}",
        f"dbt deps --target={target}",
        f"dbt build --target={target} {dbt_args} {dbt_vars}",
    ]
    task = {
        "task_key": name,
        "run_if": "ALL_SUCCESS",
        "dbt_task": {
            "source": "WORKSPACE",
            "project_directory": DBT_PROJECT_PATH,
            "commands": commands,
            "warehouse_id": SQL_WAREHOUSE_ID,
        },
        "libraries": LIBRARIES,
    }
    if compute_type == "new_cluster":
        task["new_cluster"] = CLUSTER_CONFIG
    return {"run_name": name, "tasks": [task]}


def build_elementary_vars(job_name, dag_id, run_id, use_serverless=False):
    v = {"elementary": {"job_name": job_name, "dag_id": dag_id,
                        "run_id": run_id, "use_serverless": use_serverless}}
    return f"--vars '{json.dumps(v)}'"


def on_sla_miss_callback():
    return lambda dag, task_list, blocking, slas, blocking_tis: None


def create_collect_metrics_operator(task_id, upstream_task_id):
    return PythonOperator(
        task_id=task_id,
        python_callable=lambda **ctx: None,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def create_push_metrics_operator(group_name, collect_task_id="collect_metrics"):
    return PythonOperator(
        task_id="push_metrics",
        python_callable=lambda **ctx: None,
        trigger_rule=TriggerRule.ALL_DONE,
    )


def create_latest_elementary_freshness_result_operator():
    return PythonOperator(task_id="elementary_freshness_results",
                          python_callable=lambda **ctx: None,
                          trigger_rule=TriggerRule.ALL_DONE)


def create_latest_elementary_anomaly_result_operator():
    return PythonOperator(task_id="elementary_anomaly_results",
                          python_callable=lambda **ctx: None,
                          trigger_rule=TriggerRule.ALL_DONE)
```

### 8.3 `airflow/src/dwh/dbt.py` — v1: daily only

```python
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup

import dwh.template_lib as tl
from dwh.project_lib import (
    COMMON_DAG_ARGS,
    DATABRICKS_CONNECTION_ID,
    build_elementary_vars,
    create_collect_metrics_operator,
    create_push_metrics_operator,
    generate_job_config,
    on_sla_miss_callback,
)

_FILE_NAME_STEM = Path(__file__).stem


def define_scheduled_dags():
    target = tl.env.get_environment()
    dag_id = f"{tl.PROJECT_NAME}.{_FILE_NAME_STEM}.daily"

    with DAG(
        dag_id=dag_id,
        schedule=CronTriggerTimetable("0 5 * * *", timezone="Europe/Amsterdam")
            if tl.env.on_production() else None,
        params={"DBT_TARGET": target},
        sla_miss_callback=on_sla_miss_callback(),
        **COMMON_DAG_ARGS,
    ) as dag:

        name = dag_id.replace(".", "_")
        daily_job_config = generate_job_config(
            name=name,
            dbt_args="--selector daily --exclude tag:elementary_alert tag:inactive",
            dbt_vars=build_elementary_vars(name, "{{ ti.dag_id }}", "{{ run_id }}"),
            target=target,
            compute_type="new_cluster",
        )

        with TaskGroup(group_id="dbt_daily") as dbt_daily_group:
            dbt_submit = DatabricksSubmitRunOperator(
                databricks_conn_id=DATABRICKS_CONNECTION_ID,
                task_id="run",
                run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                json=daily_job_config,
                deferrable=True,
                sla=timedelta(hours=3),
                execution_timeout=timedelta(hours=4),
                on_failure_callback=tl.on_failure_callback(),
            )
            collect = create_collect_metrics_operator("collect_metrics", "dbt_daily.run")
            push    = create_push_metrics_operator("dbt_daily")
            dbt_submit >> collect >> push

        globals()[dag_id] = dag


define_scheduled_dags()
```

**What you see in Airflow UI:**
```
poc.dbt.daily
  └── dbt_daily
        ├── run
        ├── collect_metrics
        └── push_metrics
```

One task group. The `daily` selector runs everything (all three domains + analytics + reporting)
inside that single group on an AP cluster.

---

## Phase 9 — Airflow v2: Add HR as Serverless Group

Now we move HR out of the daily run and into its own dedicated serverless group.
This requires changes in three places: `profiles.yml`, `selectors.yml`, and `dbt.py`.

### 9.1 Add serverless targets to `profiles.yml`

```yaml
# Add these two outputs alongside the existing 'udev' output:

    udev_serverless:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<serverless-warehouse-id>
      token: "{{ env_var('DBT_TOKEN') }}"
      catalog: dwh_udev
      schema: staging
      threads: 8

    upro:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<warehouse-id>
      token: "{{ env_var('DBT_TOKEN_PROD') }}"
      catalog: dwh_upro
      schema: staging
      threads: 16

    upro_serverless:
      type: databricks
      host: <your-workspace>.azuredatabricks.net
      http_path: /sql/1.0/warehouses/<serverless-warehouse-id>
      token: "{{ env_var('DBT_TOKEN_PROD') }}"
      catalog: dwh_upro
      schema: staging
      threads: 16
```

### 9.2 Update `selectors.yml` — exclude HR from daily

HR now has its own group, so exclude it from the daily selector:

```yaml
selectors:

  - name: daily
    description: "Main daily run — excludes all dedicated task groups"
    definition:
      union:
        - method: path
          value: models/
        exclude:
          - method: tag
            value: elementary_alert
          - method: tag
            value: inactive
          - method: resource_type
            value: snapshot
          - method: tag
            value: hr           # ← ADD THIS: HR now has its own task group

  # ── ADD this new selector ──────────────────────────────────────────────
  - name: hr
    description: "HR domain — serverless compute"
    definition:
      method: tag
      value: hr
```

**Verify:**
```bash
dbt ls --target udev --selector daily
# HR models should NO LONGER appear in this list

dbt ls --target udev --selector hr
# Should list: stg_hr__employees, int_hr__employee_metrics, hr__employee_summary
```

### 9.3 Update `dbt.py` — add `serverless_model_groups` dict and loop

Add the following to `define_scheduled_dags()`, after the `dbt_daily_group` block:

```python
        # serverless_model_groups: each entry produces one independent task group
        # that runs on serverless compute, in parallel with all other groups,
        # after dbt_daily.
        serverless_model_groups = {
            "hr": ("hr", timedelta(hours=1), timedelta(hours=1, minutes=30), False),
        }

        dual_run_model_groups = {}   # empty for now — payments will go here in Phase 12

        # Accumulates every group that must finish before elementary tests run
        model_build_task_groups = [dbt_daily_group]

        for group_name, (selector_name, group_sla, group_timeout, use_new_dbt) in serverless_model_groups.items():

            group_name_full = f"{dag_id.replace('.', '_')}_{group_name}"
            group_target    = f"{target}_serverless"

            group_config = generate_job_config(
                name=group_name_full,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(
                    group_name_full, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=True
                ),
                target=group_target,
                compute_type="serverless",
                use_new_dbt=use_new_dbt,
            )

            with TaskGroup(group_id=f"dbt_{group_name}") as group_task_group:
                group_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=group_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                g_collect = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}.run")
                g_push    = create_push_metrics_operator(f"dbt_{group_name}")
                group_task >> g_collect >> g_push

            dbt_daily_group >> group_task_group
            model_build_task_groups.append(group_task_group)
```

Add the missing import at the top of the file:
```python
from airflow.utils.trigger_rule import TriggerRule
```

**What you now see in Airflow UI:**
```
poc.dbt.daily
  ├── dbt_daily      (AP cluster, runs everything except hr)
  └── dbt_hr         (serverless, runs only --selector hr)
```

---

## Phase 10 — Airflow v3: Add Marketplace as Serverless Group

Adding a second serverless group is ONE dict entry.

### 10.1 Update `selectors.yml` — exclude marketplace from daily

```yaml
        exclude:
          ...
          - method: tag
            value: hr
          - method: tag
            value: marketplace    # ← ADD THIS

  # ── ADD this new selector ──────────────────────────────────────────────
  - name: marketplace
    description: "Marketplace domain — serverless compute"
    definition:
      method: tag
      value: marketplace
```

### 10.2 Update `dbt.py` — add marketplace to the dict

```python
        serverless_model_groups = {
            "hr":          ("hr",          timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "marketplace": ("marketplace", timedelta(hours=1),        timedelta(hours=1, minutes=30), False),  # ← ADD
        }
```

That's it. The loop handles the rest.

**What you now see in Airflow UI:**
```
poc.dbt.daily
  ├── dbt_daily       (AP cluster)
  ├── dbt_hr          (serverless)
  └── dbt_marketplace (serverless)   ← new, from the same loop
```

---

## Phase 11 — Airflow v4: Add Analytics Layer as Serverless Group

Same pattern again.

### 11.1 Update `selectors.yml`

```yaml
        exclude:
          ...
          - method: tag
            value: marketplace
          - method: tag
            value: analytics       # ← ADD
          - method: tag
            value: reporting       # ← ADD

  # ── ADD these two selectors ────────────────────────────────────────────
  - name: analytics_layer
    description: "Cross-domain analytics and reporting — runs after all marts"
    definition:
      union:
        - method: tag
          value: analytics
        - method: tag
          value: reporting
```

### 11.2 Update `dbt.py` — add analytics_layer to the dict

```python
        serverless_model_groups = {
            "hr":              ("hr",              timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "marketplace":     ("marketplace",     timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "analytics_layer": ("analytics_layer", timedelta(hours=1, minutes=30), timedelta(hours=2),        False),  # ← ADD
        }
```

**What you now see in Airflow UI:**
```
poc.dbt.daily
  ├── dbt_daily           (AP cluster)
  ├── dbt_hr              (serverless)
  ├── dbt_marketplace     (serverless)
  └── dbt_analytics_layer (serverless)
```

---

## Phase 12 — Airflow v5: Add Payments as Dual-Run Group

This phase is different. Payments goes into `dual_run_model_groups`, which runs
a DIFFERENT loop — one that creates TWO task groups per entry (AP + serverless).

### 12.1 Bootstrap the shadow table in Databricks

Before enabling the dual-run, the shadow table must exist so the first serverless
incremental run doesn't do a full history load.

Create `sql/adhoc_sql.sql`:
```sql
-- Run once in Databricks SQL editor before enabling dual-run in dbt.py
DROP TABLE IF EXISTS dwh_udev.payments.pay__transaction_summary_serverless;
CREATE TABLE dwh_udev.payments.pay__transaction_summary_serverless
SHALLOW CLONE dwh_udev.payments.pay__transaction_summary;
```

Run it in the Databricks SQL editor.

### 12.2 Update `get_serverless_alias.sql` — add payments paths

```sql
{#
    Currently in dual-run:
    - payments: staging/payments/, intermediate/payments/, marts/payments/
#}
{% macro generate_alias_name(custom_alias_name="", node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node is not none -%}
        {%- set is_serverless_target = target.name.endswith('_serverless') -%}
        {%- set is_selected_model = (
            node.resource_type == "model"
            and node.path is defined
            and (
                node.path.startswith("staging/payments/")
                or node.path.startswith("intermediate/payments/")
                or node.path.startswith("marts/payments/")
            )
        ) -%}
        {%- if is_selected_model and is_serverless_target -%}
            {{ node.name }}_serverless
        {%- else -%}
            {{ node.name }}
        {%- endif -%}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
```

**Verify the alias macro works:**
```bash
dbt build --target udev_serverless --select tag:payments
# → Creates dwh_udev.payments.pay__transaction_summary_serverless
# → Does NOT touch dwh_udev.payments.pay__transaction_summary
```

### 12.3 Update `selectors.yml` — exclude payments from daily

```yaml
        exclude:
          ...
          - method: tag
            value: analytics
          - method: tag
            value: reporting
          - method: tag
            value: payments       # ← ADD

  # ── ADD this selector ──────────────────────────────────────────────────
  - name: payments
    description: "Payments domain — dual-run AP + serverless"
    definition:
      method: tag
      value: payments
```

### 12.4 Update `dbt.py` — add dual_run_model_groups loop

Add payments to `dual_run_model_groups` and add the loop that processes it.

```python
        dual_run_model_groups = {
            "payments": ("payments", timedelta(hours=1), timedelta(hours=1, minutes=30), False),  # ← ADD
        }
```

Now add the dual-run loop **after** the serverless loop:

```python
        for group_name, (selector_name, group_sla, group_timeout, use_new_dbt) in dual_run_model_groups.items():

            # AP task group — runs on AP cluster, writes canonical table names
            ap_name = f"{dag_id.replace('.', '_')}_{group_name}_ap"
            ap_config = generate_job_config(
                name=ap_name,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(ap_name, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=False),
                target=target,
                compute_type="new_cluster",
                use_new_dbt=use_new_dbt,
            )

            with TaskGroup(group_id=f"dbt_{group_name}_ap") as ap_group:
                ap_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=ap_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                ap_c = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}_ap.run")
                ap_p = create_push_metrics_operator(f"dbt_{group_name}_ap")
                ap_task >> ap_c >> ap_p

            dbt_daily_group >> ap_group
            model_build_task_groups.append(ap_group)

            # Serverless task group — runs on serverless, writes _serverless shadow tables
            sl_name = f"{dag_id.replace('.', '_')}_{group_name}_serverless"
            sl_config = generate_job_config(
                name=sl_name,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(sl_name, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=True),
                target=f"{target}_serverless",
                compute_type="serverless",
                use_new_dbt=use_new_dbt,
            )

            with TaskGroup(group_id=f"dbt_{group_name}_serverless") as sl_group:
                sl_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=sl_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                sl_c = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}_serverless.run")
                sl_p = create_push_metrics_operator(f"dbt_{group_name}_serverless")
                sl_task >> sl_c >> sl_p

            dbt_daily_group >> sl_group
            model_build_task_groups.append(sl_group)
```

**What you now see in Airflow UI:**
```
poc.dbt.daily
  ├── dbt_daily                (AP cluster)
  ├── dbt_hr                   (serverless)
  ├── dbt_marketplace          (serverless)
  ├── dbt_analytics_layer      (serverless)
  ├── dbt_payments_ap          (AP cluster — canonical table)
  └── dbt_payments_serverless  (serverless — _serverless shadow table)
```

This is the target shape. Five groups running in parallel after `dbt_daily`.

---

## Phase 13 — Airflow v6: Wire in Elementary Alert Tests

Elementary tests run after ALL groups complete. This is the "join point" —
every group feeds into one final task.

### 13.1 Update `dbt.py` — add elementary block

Add this **after** both loops, at the end of `define_scheduled_dags()`:

```python
        from dwh.project_lib import (
            create_latest_elementary_freshness_result_operator,
            create_latest_elementary_anomaly_result_operator,
        )

        alert_name = f"{dag_id.replace('.', '_')}_elementary_alert_tests"
        elementary_alert_task = DatabricksSubmitRunOperator(
            databricks_conn_id=DATABRICKS_CONNECTION_ID,
            task_id="dbt_elementary_alert_tests",
            run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
            json=generate_job_config(
                name=alert_name,
                dbt_args="--select tag:elementary_alert",
                dbt_vars=build_elementary_vars(alert_name, "{{ ti.dag_id }}", "{{ run_id }}"),
                target=target,
                compute_type="new_cluster",
            ),
            deferrable=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        # Wire every task group → elementary (the join point)
        for task_group in model_build_task_groups:
            task_group >> elementary_alert_task

        freshness_task = create_latest_elementary_freshness_result_operator()
        anomaly_task   = create_latest_elementary_anomaly_result_operator()
        elementary_alert_task >> freshness_task
        elementary_alert_task >> anomaly_task
```

**Final Airflow DAG shape:**
```
poc.dbt.daily
  ├── dbt_daily ───────────────────────────────────────────────┐
  ├── dbt_hr ──────────────────────────────────────────────────┤
  ├── dbt_marketplace ─────────────────────────────────────────┤──► elementary_alert_tests
  ├── dbt_analytics_layer ─────────────────────────────────────┤         ├── freshness_results
  ├── dbt_payments_ap ─────────────────────────────────────────┤         └── anomaly_results
  └── dbt_payments_serverless ──────────────────────────────────┘
```

---

## Phase 14 — DWH Internal Validation Models

These models let you compare the AP canonical table vs the serverless shadow table.
They are gated by a var and only run when you explicitly enable them.

### 14.1 Update `dbt_project.yml` — add dwh_internal section

```yaml
# Add under models > poc_dwh:
    dwh_internal:
      +schema: dwh_internal
      +materialized: view
      serverless_validation_row_counts:
        +enabled: "{{ var('run_serverless_validation', false) }}"
      serverless_validation_schema_check:
        +enabled: "{{ var('run_serverless_validation', false) }}"
      serverless_validation_combined:
        +enabled: "{{ var('run_serverless_validation', false) }}"
```

### 14.2 Validation models

`dbt/models/dwh_internal/serverless_validation_row_counts.sql`:
```sql
{{
    config(
        materialized='view',
        enabled="{{ var('run_serverless_validation', false) }}"
    )
}}

with ap_counts as (
    select event_date, count(*) as row_count
    from {{ ref('pay__transaction_summary') }}
    group by event_date
),
serverless_counts as (
    select event_date, count(*) as row_count
    from {{ target.catalog }}.payments.pay__transaction_summary_serverless
    group by event_date
),
comparison as (
    select
        coalesce(a.event_date, s.event_date)    as event_date,
        coalesce(a.row_count, 0)                as ap_row_count,
        coalesce(s.row_count, 0)                as serverless_row_count,
        coalesce(a.row_count, 0)
            - coalesce(s.row_count, 0)          as diff,
        case
            when coalesce(a.row_count, 0) = coalesce(s.row_count, 0) then 'MATCH'
            else 'MISMATCH'
        end                                     as status
    from ap_counts as a
    full outer join serverless_counts as s using (event_date)
)
select * from comparison
order by event_date desc
```

`dbt/models/dwh_internal/serverless_validation_schema_check.sql`:
```sql
{{
    config(
        materialized='view',
        enabled="{{ var('run_serverless_validation', false) }}"
    )
}}

select
    ap.column_name,
    ap.data_type        as ap_data_type,
    sl.data_type        as serverless_data_type,
    case
        when ap.data_type = sl.data_type then 'MATCH'
        when sl.data_type is null        then 'MISSING_IN_SERVERLESS'
        when ap.data_type is null        then 'MISSING_IN_AP'
        else 'TYPE_MISMATCH'
    end                 as status
from information_schema.columns as ap
full outer join information_schema.columns as sl
    on  ap.column_name   = sl.column_name
    and ap.table_catalog = sl.table_catalog
    and ap.table_schema  = 'payments'
where ap.table_name = 'pay__transaction_summary'
  and sl.table_name = 'pay__transaction_summary_serverless'
```

`dbt/models/dwh_internal/serverless_validation_combined.sql`:
```sql
{{
    config(
        materialized='view',
        enabled="{{ var('run_serverless_validation', false) }}"
    )
}}

select 'row_counts'   as check_type, * from {{ ref('serverless_validation_row_counts') }}
union all
select 'schema_check' as check_type, * from {{ ref('serverless_validation_schema_check') }}
```

### 14.3 Run validation

```bash
dbt run --target udev --select dwh_internal \
  --vars '{"run_serverless_validation": true}'
```

**Query the result:**
```sql
SELECT * FROM dwh_udev.dwh_internal.serverless_validation_combined
WHERE status != 'MATCH';
-- Empty result = outputs match
```

---

## Phase 15 — CI/CD

### 15.1 `databricks.yml` (bundle definition)

This file declares the two deployment targets. When you have a single workspace (this POC),
both targets point to the same host — the only difference is the catalog dbt writes to
(`dwh_udev` vs `dwh_upro`). When you provision a second workspace, change the `prod` host
and drop in the service principal. Nothing else in the project moves.

```yaml
bundle:
  name: poc-dwh

targets:

  dev:
    mode: development
    default: true
    workspace:
      host: https://<your-workspace>.azuredatabricks.net

  prod:
    mode: production
    workspace:
      host: https://<your-workspace>.azuredatabricks.net
      # Two-workspace upgrade: replace the host above with the prod workspace URL.
      # Add the real service principal name below.
    run_as:
      service_principal_name: <prod-sp>@<tenant>.com
```

### 15.2 `.github/workflows/ci.yml`
```yaml
name: CI

on:
  pull_request:
    branches: [master]
    paths: ['dbt/**', 'airflow/**']
  push:
    branches: [master]
    paths: ['dbt/**', 'airflow/**']

jobs:
  dbt-checks:
    name: dbt compile + lint
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install dbt-databricks==1.8.7 sqlfluff==3.0.0 elementary-data==0.14.3
      - name: dbt deps
        working-directory: dbt
        run: dbt deps
        env:
          DBT_TOKEN: ${{ secrets.DBT_TOKEN_UDEV }}
      - name: dbt compile
        working-directory: dbt
        run: dbt compile --target udev
        env:
          DBT_TOKEN: ${{ secrets.DBT_TOKEN_UDEV }}
      - name: sqlfluff lint
        working-directory: dbt
        run: sqlfluff lint models/ --dialect databricks --processes 4

  deploy-prod:
    name: Deploy to production
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/master'
    steps:
      - uses: actions/checkout@v4
      - run: pip install databricks-cli
      - run: databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST_PROD }}
          DATABRICKS_TOKEN: ${{ secrets.DBT_TOKEN_PROD }}
```

---

## Phase 16 — Cut Payments to Serverless-Only

Once validation shows MATCH for several days, payments leaves dual-run.

### 16.1 `dbt.py` — move payments from `dual_run_model_groups` to `serverless_model_groups`

```python
        serverless_model_groups = {
            "hr":              ("hr",              timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "marketplace":     ("marketplace",     timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "analytics_layer": ("analytics_layer", timedelta(hours=1, minutes=30), timedelta(hours=2),        False),
            "payments":        ("payments",        timedelta(hours=1),        timedelta(hours=1, minutes=30), False),  # ← moved here
        }

        dual_run_model_groups = {}   # ← now empty again
```

### 16.2 `get_serverless_alias.sql` — remove payments paths

```sql
{#
    Currently in dual-run:
    (none — payments completed validation)
#}
{% macro generate_alias_name(custom_alias_name="", node=none) -%}
    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node is not none -%}
        {{ node.name }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{%- endmacro %}
```

### 16.3 Rename tables in Databricks

```sql
-- Swap: the serverless-built table becomes the canonical one
ALTER TABLE dwh_upro.payments.pay__transaction_summary
    RENAME TO dwh_upro.payments.pay__transaction_summary_ap_backup;

ALTER TABLE dwh_upro.payments.pay__transaction_summary_serverless
    RENAME TO dwh_upro.payments.pay__transaction_summary;

-- After a few days of stable serverless-only runs:
DROP TABLE dwh_upro.payments.pay__transaction_summary_ap_backup;
```

**Final Airflow DAG shape (after cutover):**
```
poc.dbt.daily
  ├── dbt_daily            (AP cluster)
  ├── dbt_hr               (serverless)
  ├── dbt_marketplace      (serverless)
  ├── dbt_analytics_layer  (serverless)
  └── dbt_payments         (serverless — now identical to hr and marketplace)
          └── elementary_alert_tests
```

---

## Complete `dbt.py` — Final State for Reference

After all phases, this is what the complete file looks like:

```python
from datetime import timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

import dwh.template_lib as tl
from dwh.project_lib import (
    COMMON_DAG_ARGS,
    DATABRICKS_CONNECTION_ID,
    build_elementary_vars,
    create_collect_metrics_operator,
    create_latest_elementary_anomaly_result_operator,
    create_latest_elementary_freshness_result_operator,
    create_push_metrics_operator,
    generate_job_config,
    merge_dbt_vars,
    on_sla_miss_callback,
)

_FILE_NAME_STEM = Path(__file__).stem


def define_scheduled_dags():
    target = tl.env.get_environment()
    dag_id = f"{tl.PROJECT_NAME}.{_FILE_NAME_STEM}.daily"

    with DAG(
        dag_id=dag_id,
        schedule=CronTriggerTimetable("0 5 * * *", timezone="Europe/Amsterdam")
            if tl.env.on_production() else None,
        params={"DBT_TARGET": target},
        sla_miss_callback=on_sla_miss_callback(),
        **COMMON_DAG_ARGS,
    ) as dag:

        # ── dbt_daily ────────────────────────────────────────────────────────
        name = dag_id.replace(".", "_")
        daily_config = generate_job_config(
            name=name,
            dbt_args="--selector daily --exclude tag:elementary_alert tag:inactive",
            dbt_vars=build_elementary_vars(name, "{{ ti.dag_id }}", "{{ run_id }}"),
            target=target,
            compute_type="new_cluster",
        )
        with TaskGroup(group_id="dbt_daily") as dbt_daily_group:
            dbt_submit = DatabricksSubmitRunOperator(
                databricks_conn_id=DATABRICKS_CONNECTION_ID,
                task_id="run",
                run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                json=daily_config,
                deferrable=True,
                sla=timedelta(hours=3),
                execution_timeout=timedelta(hours=4),
                on_failure_callback=tl.on_failure_callback(),
            )
            collect = create_collect_metrics_operator("collect_metrics", "dbt_daily.run")
            push    = create_push_metrics_operator("dbt_daily")
            dbt_submit >> collect >> push

        # ── Serverless groups (one task group per entry) ─────────────────────
        serverless_model_groups = {
            "hr":              ("hr",              timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "marketplace":     ("marketplace",     timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
            "analytics_layer": ("analytics_layer", timedelta(hours=1, minutes=30), timedelta(hours=2),        False),
            "payments":        ("payments",        timedelta(hours=1),        timedelta(hours=1, minutes=30), False),
        }

        # ── Dual-run groups (two task groups per entry: AP + serverless) ──────
        dual_run_model_groups = {}

        model_build_task_groups = [dbt_daily_group]

        for group_name, (selector_name, group_sla, group_timeout, use_new_dbt) in serverless_model_groups.items():
            group_name_full = f"{dag_id.replace('.', '_')}_{group_name}"
            group_target    = f"{target}_serverless"
            group_config = generate_job_config(
                name=group_name_full,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(group_name_full, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=True),
                target=group_target,
                compute_type="serverless",
                use_new_dbt=use_new_dbt,
            )
            with TaskGroup(group_id=f"dbt_{group_name}") as group_task_group:
                group_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=group_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                g_c = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}.run")
                g_p = create_push_metrics_operator(f"dbt_{group_name}")
                group_task >> g_c >> g_p
            dbt_daily_group >> group_task_group
            model_build_task_groups.append(group_task_group)

        for group_name, (selector_name, group_sla, group_timeout, use_new_dbt) in dual_run_model_groups.items():
            ap_name = f"{dag_id.replace('.', '_')}_{group_name}_ap"
            ap_config = generate_job_config(
                name=ap_name,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(ap_name, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=False),
                target=target,
                compute_type="new_cluster",
                use_new_dbt=use_new_dbt,
            )
            with TaskGroup(group_id=f"dbt_{group_name}_ap") as ap_group:
                ap_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=ap_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                ap_c = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}_ap.run")
                ap_p = create_push_metrics_operator(f"dbt_{group_name}_ap")
                ap_task >> ap_c >> ap_p
            dbt_daily_group >> ap_group
            model_build_task_groups.append(ap_group)

            sl_name = f"{dag_id.replace('.', '_')}_{group_name}_serverless"
            sl_config = generate_job_config(
                name=sl_name,
                dbt_args=f"--selector {selector_name} --exclude tag:elementary_alert tag:inactive",
                dbt_vars=build_elementary_vars(sl_name, "{{ ti.dag_id }}", "{{ run_id }}", use_serverless=True),
                target=f"{target}_serverless",
                compute_type="serverless",
                use_new_dbt=use_new_dbt,
            )
            with TaskGroup(group_id=f"dbt_{group_name}_serverless") as sl_group:
                sl_task = DatabricksSubmitRunOperator(
                    databricks_conn_id=DATABRICKS_CONNECTION_ID,
                    task_id="run",
                    run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
                    json=sl_config,
                    deferrable=True,
                    trigger_rule=TriggerRule.ALL_DONE,
                    sla=group_sla,
                    execution_timeout=group_timeout,
                    on_failure_callback=tl.on_failure_callback(),
                )
                sl_c = create_collect_metrics_operator("collect_metrics", f"dbt_{group_name}_serverless.run")
                sl_p = create_push_metrics_operator(f"dbt_{group_name}_serverless")
                sl_task >> sl_c >> sl_p
            dbt_daily_group >> sl_group
            model_build_task_groups.append(sl_group)

        # ── Elementary alert tests (join point) ──────────────────────────────
        alert_name = f"{dag_id.replace('.', '_')}_elementary_alert_tests"
        elementary_alert_task = DatabricksSubmitRunOperator(
            databricks_conn_id=DATABRICKS_CONNECTION_ID,
            task_id="dbt_elementary_alert_tests",
            run_name="{{ ti.dag_id }}-{{ ti.task_id }}",
            json=generate_job_config(
                name=alert_name,
                dbt_args="--select tag:elementary_alert",
                dbt_vars=build_elementary_vars(alert_name, "{{ ti.dag_id }}", "{{ run_id }}"),
                target=target,
                compute_type="new_cluster",
            ),
            deferrable=True,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        for task_group in model_build_task_groups:
            task_group >> elementary_alert_task

        freshness_task = create_latest_elementary_freshness_result_operator()
        anomaly_task   = create_latest_elementary_anomaly_result_operator()
        elementary_alert_task >> freshness_task
        elementary_alert_task >> anomaly_task

        globals()[dag_id] = dag


define_scheduled_dags()
```

---

## Phase 17 — Cost Monitoring Dashboard

This phase ties the whole project together. You'll build a dashboard that shows
the real cost of every decision you made in Phases 0-16 — and makes concepts
like "serverless is cheaper" and "dual-run doubles cost temporarily" visible
as actual numbers.

### Core concept: DBU (Databricks Unit)

Everything in Databricks is billed in DBUs. But 1 DBU is not 1 DBU —
different SKUs have different prices per DBU:

| SKU | When it applies | Relative cost |
|---|---|---|
| `JOBS_CLASSIC_COMPUTE` | AP cluster running a job | Highest |
| `JOBS_SERVERLESS_COMPUTE` | Serverless job compute | Lower |
| `SQL_CLASSIC_COMPUTE` | Classic SQL warehouse | Medium |
| `SQL_SERVERLESS_COMPUTE` | Serverless SQL warehouse | Lower |
| `ALL_PURPOSE_COMPUTE` | Interactive cluster, notebooks | Highest |

In this POC:
- `dbt_daily` uses `JOBS_CLASSIC_COMPUTE` (AP cluster)
- `dbt_hr`, `dbt_marketplace`, `dbt_analytics_layer`, `dbt_payments` use `JOBS_SERVERLESS_COMPUTE`
- During Phase 12, `dbt_payments_ap` + `dbt_payments_serverless` both run — costs add together
- After Phase 16 cutover: only `dbt_payments` (serverless) remains

### 17.1 Enable system tables

Run in Databricks SQL editor (requires account admin or metastore admin):

```sql
ALTER METASTORE <your-metastore-id> ENABLE SYSTEM SCHEMA billing;
ALTER METASTORE <your-metastore-id> ENABLE SYSTEM SCHEMA lakeflow;
ALTER METASTORE <your-metastore-id> ENABLE SYSTEM SCHEMA compute;
```

Verify:
```sql
SHOW SCHEMAS IN system;
-- Should include: billing, lakeflow, compute
```

### 17.2 Explore the raw system tables first

Before building the dashboard, run these to understand the shape of the data.

**What did I spend this week?**
```sql
SELECT
    usage_date,
    sku_name,
    round(sum(usage_quantity), 2)  AS total_dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - 7
  AND workspace_id = '<your-workspace-id>'
GROUP BY 1, 2
ORDER BY usage_date DESC, total_dbus DESC;
```

**Which jobs ran and how long?**
```sql
SELECT
    job_id,
    run_id,
    result_state,
    period_start_time,
    period_end_time,
    datediff('second', period_start_time, period_end_time)  AS duration_seconds
FROM system.lakeflow.job_run_timeline
WHERE period_start_time >= current_date() - 7
ORDER BY period_start_time DESC
LIMIT 50;
```

**Tasks within a specific job run:**
```sql
SELECT
    task_key,
    result_state,
    period_start_time,
    period_end_time,
    datediff('second', period_start_time, period_end_time)  AS duration_seconds
FROM system.lakeflow.task_run_timeline
WHERE job_id = '<your-job-id>'
ORDER BY period_start_time DESC;
```

### 17.3 Dashboard tiles

Create a Databricks SQL Lakeview dashboard named `POC DWH — Cost & Performance`.
Add a `usage_date >= :start_date` parameter and wire it to all tiles.

---

**Tile 1 — Daily DBU spend by compute type** (line chart)

```sql
SELECT
    usage_date,
    CASE
        WHEN sku_name LIKE '%SERVERLESS%' THEN 'Serverless'
        WHEN sku_name LIKE '%CLASSIC%'    THEN 'Classic (AP)'
        ELSE 'Other'
    END                                AS compute_type,
    round(sum(usage_quantity), 4)      AS total_dbus
FROM system.billing.usage
WHERE usage_date >= :start_date
  AND workspace_id = '<your-workspace-id>'
GROUP BY 1, 2
ORDER BY 1;
```

What to look for: After Phase 9, the Classic line drops and the Serverless line rises.
If total area under both lines decreases, the migration is saving money.

---

**Tile 2 — Cost per dbt task group** (bar chart, x=run_name, y=total_dbus, series=compute_type)

```sql
SELECT
    custom_tags['RunName']             AS run_name,
    usage_date,
    CASE
        WHEN sku_name LIKE '%SERVERLESS%' THEN 'serverless'
        ELSE 'classic'
    END                                AS compute_type,
    round(sum(usage_quantity), 4)      AS total_dbus
FROM system.billing.usage
WHERE usage_date >= :start_date
  AND workspace_id = '<your-workspace-id>'
  AND custom_tags['RunName'] IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY usage_date DESC, total_dbus DESC;
```

What to look for: During Phase 12, both `dbt_payments_ap` and `dbt_payments_serverless`
bars appear — combined height is roughly double the cost of a single group.

---

**Tile 3 — Run duration vs DBU cost** (table)

```sql
WITH runs AS (
    SELECT
        run_id,
        result_state,
        period_start_time,
        datediff('minute', min(period_start_time), max(period_end_time))  AS duration_minutes
    FROM system.lakeflow.job_run_timeline
    WHERE period_start_time >= :start_date
    GROUP BY 1, 2, 3
),
billing AS (
    SELECT
        cast(custom_tags['RunId'] AS BIGINT)  AS run_id,
        round(sum(usage_quantity), 4)          AS total_dbus,
        max(sku_name)                          AS sku_name
    FROM system.billing.usage
    WHERE usage_date >= :start_date
      AND custom_tags['RunId'] IS NOT NULL
    GROUP BY 1
)
SELECT
    r.run_id,
    r.result_state,
    r.duration_minutes,
    b.total_dbus,
    b.sku_name,
    CASE WHEN r.duration_minutes > 0
         THEN round(b.total_dbus / r.duration_minutes, 4)
         ELSE 0
    END                                  AS dbus_per_minute
FROM runs  AS r
JOIN billing AS b ON r.run_id = b.run_id
ORDER BY r.period_start_time DESC;
```

What to look for: Serverless jobs start faster (no cluster spin-up).
Lower `dbus_per_minute` AND shorter `duration_minutes` = lower total cost.

---

**Tile 4 — Full-refresh vs incremental cost** (bar chart)

To make this tile useful, tag your runs before executing them:
```bash
# Run with a tag so you can filter by it later
dbt build --target udev --select tag:hr --full-refresh \
  --vars '{"run_type": "full_refresh", "domain": "hr"}'

dbt build --target udev --select tag:hr \
  --vars '{"run_type": "incremental", "domain": "hr"}'
```

```sql
SELECT
    usage_date,
    custom_tags['run_type']            AS run_type,
    custom_tags['domain']              AS domain,
    round(sum(usage_quantity), 4)      AS total_dbus
FROM system.billing.usage
WHERE usage_date >= :start_date
  AND custom_tags['domain'] IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1 DESC;
```

What to look for: Full-refresh typically costs 5-10x a regular incremental run.
This is the quantified argument for why `replace_where` with self-healing windows matters.

---

**Tile 5 — Dual-run cost impact** (stacked area chart)

```sql
SELECT
    usage_date,
    round(SUM(CASE WHEN custom_tags['RunName'] LIKE '%payments_ap%'
                   THEN usage_quantity ELSE 0 END), 4)  AS payments_ap_dbus,
    round(SUM(CASE WHEN custom_tags['RunName'] LIKE '%payments_serverless%'
                   THEN usage_quantity ELSE 0 END), 4)  AS payments_serverless_dbus
FROM system.billing.usage
WHERE usage_date >= :start_date
  AND workspace_id = '<your-workspace-id>'
GROUP BY 1
ORDER BY 1;
```

What to look for across all three phases:
- **Before Phase 12:** `payments_ap_dbus` line only
- **During Phase 12:** both lines active — total doubles
- **After Phase 16:** only `payments_serverless_dbus` — and it should be lower than the original AP value

This one chart tells the story of the entire serverless migration.

### 17.4 What each tile teaches

| Tile | Concept |
|---|---|
| 1 — Daily by compute type | Different SKUs have different rates; migration shifts spend from Classic to Serverless |
| 2 — Cost per task group | Serverless groups cost less per run than AP groups; every group has a real cost |
| 3 — Duration vs DBU | No cluster spin-up = shorter wall-clock = lower total DBU even at same rate |
| 4 — Full-refresh vs incremental | `replace_where` and self-healing windows directly reduce compute spend |
| 5 — Dual-run impact | Migration temporarily doubles cost; cutover removes the overhead permanently |

### 17.5 Real project cost controls to know about

| Control | Why it matters |
|---|---|
| Cluster policies | Force minimum node config to prevent dev teams from spinning up oversized clusters |
| Photon | More expensive per DBU but runs faster — worth it for large scans, NOT for small incremental runs |
| Warehouse autostop | Set to 5-10 min on dev warehouses; production can be longer |
| Spot instances | Save ~70% on AP cluster compute — risky for production scheduled jobs due to preemption |
| Partition pruning | If `replace_where` predicates don't match the model's WHERE clause, you scan all partitions — verify with `EXPLAIN` |
| Cost attribution tags | Tag every job run with domain + environment + run_type — Tile 2 only works at scale if these tags are consistently applied |
