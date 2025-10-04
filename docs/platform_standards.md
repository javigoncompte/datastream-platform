# Table of Contents
- [Table of Contents](#table-of-contents)
- [Data Engineering Standards](#data-engineering-standards)
  - [Purpose \& Scope](#purpose--scope)
  - [1) Naming Conventions](#1-naming-conventions)
    - [1.1 Tables (Unity Catalog / Delta)](#11-tables-unity-catalog--delta)
    - [1.2 Files \& Modules](#12-files--modules)
    - [1.3 Functions \& Methods](#13-functions--methods)
    - [1.4 Variables](#14-variables)
  - [2) Repository Layout](#2-repository-layout)
  - [3) Code Quality Rules](#3-code-quality-rules)
    - [3.1 PySpark/Databricks specifics](#31-pysparkdatabricks-specifics)
  - [4) Task vs Transform Pattern](#4-task-vs-transform-pattern)
  - [5) QA: One YAML per Table (Silver, Gold, Feature)](#5-qa-one-yaml-per-table-silver-gold-feature)
    - [5.1 Location \& Naming](#51-location--naming)
    - [5.2 Minimum Required Checks](#52-minimum-required-checks)
    - [5.3 YAML Template (DQX‑style)](#53-yaml-template-dqxstyle)
  - [6) Pull Requests \& Reviews](#6-pull-requests--reviews)
  - [7) Databricks Jobs/Workflows](#7-databricks-jobsworkflows)
  - [8) Documentation \& Discoverability](#8-documentation--discoverability)
- [Gold Table Standards](#gold-table-standards)
    - [9.1 Table Naming](#91-table-naming)
    - [9.2 Date Fields](#92-date-fields)
    - [9.3 Numeric Fields](#93-numeric-fields)
    - [9.4 Column Naming](#94-column-naming)
    - [9.5 Aggregations](#95-aggregations)
    - [9.6 Data Quality \& Governance](#96-data-quality--governance)
    - [9.7 Documentation](#97-documentation)
  - [10) Examples: Do / Don’t](#10-examples-do--dont)
  - [11) Non‑Negotiables (Quick Checklist)](#11-nonnegotiables-quick-checklist)

# Data Engineering Standards


## Purpose & Scope
A concise playbook for how we name, structure, and ship data engineering code and tables at DataPlatform. These standards reduce cognitive load, improve reliability, and make promotion‑grade work repeatable across teams (Databricks, PySpark, DLT/Workflows, dbt, QA).

---

## 1) Naming Conventions

### 1.1 Tables (Unity Catalog / Delta)
- **Singular table names**: `member`, `lead`, `personal_training`, `membership`, `pricing_calendar`.
- Layer + schema + object: `bronze.<domain>.<table>`, `silver.<domain>.<table>`, `gold.<domain>.<table>`.
- **No abbreviations or acronyms** unless industry‑standard and self‑evident (prefer `personal_training` over `pt`).
- Historical/SCD tables: suffix with role, not mechanics, e.g., `membership_history` (not `membership_scd2`).
- Change data feed enabled where applicable: `TBLPROPERTIES (delta.enableChangeDataFeed = true)`.

### 1.2 Files & Modules
- **Python file names are nouns** describing what the module *is*: `membership_transform.py`, `pricing_calendar_loader.py`, `personal_training_rules.py`, `qa_runner.py`.
- Package/module naming: all‑lowercase, words separated by underscores.

### 1.3 Functions & Methods
- **Function names are verbs** describing what the function *does*: `load_membership()`, `deduplicate_membership()`, `apply_scd2()`, `merge_into_delta()`, `get_member_activity()`.
- If a function segment becomes complex, **extract** it into a focused helper function with a verb name: `compute_modified_date()`, `resolve_conflicts()`, `sanitize_columns()`.

### 1.4 Variables
- **DataFrames must end in `_df`**: `source_df`, `staged_df`, `silver_member_df`.
- Avoid abbreviations: prefer `personal_training_df` over `pt_df`.

---

## 2) Repository Layout
A clear separation between orchestration, transforms, and QA.

```
repo_root/
  dataplatform/
    <domain>/
      transforms/               # Complex business logic lives here
        membership_transform.py
        personal_training_transform.py
      task/                     # Thin task wrappers that call into transforms
        membership_task.py
        personal_training_task.py
      qa/                       # One YAML per table (silver/gold/feature)
        silver_membership.yml
        gold_member_kpis.yml
        feature_member_embeddings.yml   
```

**Rule:** Each file in `/task` is simplified; the complexity lives in `/transforms`.

---

## 3) Code Quality Rules
- **Remove unnecessary comments** and **delete commented‑out code** before merging.
- Prefer pure functions: deterministic in/out, minimal side effects.
- Log key steps and row counts at boundaries (load → transform → write): `logger.info()`
- Try to wrap logging in functions in order to keep the code free from logging statement pollution.
- Add type hints for function signatures.
- Explicitly declare the datatype a function is returning: `def() -> DataFrame` over `def()`
- **Remove unused code**: functions, imports, and variables not actively used should be deleted, not left dormant. This keeps the codebase lean and maintainable.
- **Rule of 3 for abstraction**: don’t extract constants, helpers, or abstractions until the same code pattern appears at least three times. Example: only define shared constants for merge history once they are referenced across multiple functions.

### 3.1 PySpark/Databricks specifics
- **Do not** `import pyspark.sql.functions as F`. Import only the functions used: `from pyspark.sql.functions import col, when, coalesce, sum as sum_`.
- Avoid `.collect()` on large datasets. Use `limit()` + display/`take()` for debugging or aggregate checks.
- Partitioning: choose partition columns deliberately (e.g., by `date` or `club_id` where appropriate). Avoid over‑partitioning small tables.
- Use broadcast joins appropriately and when performance gains are noticably improved.
- Cache/persist only when it materially reduces recomputation; unpersist when done.
- Writes: use **idempotent** patterns and Delta `MERGE` via a client abstraction (e.g., `ManagedTableClient`).

---

## 4) Task vs Transform Pattern

**/task** modules:
- Boilerplate wrappers invoked by Databricks Jobs/DLT.
- Parse arguments, load configs, and delegate to transforms or modules.
- Handle only orchestration (calling transforms, success/failure, audit, metrics).

**/transforms** modules:
- Hold all non‑trivial logic: cleansing, joins, SCD2, CDC, business rules, deduping.
- Expose small, composable functions (verbs) with clear inputs/outputs.

**Example**
```python
# task/membership_task.py  (noun file; thin wrapper)
from dataplatform.membership.transforms.membership_transform import build_silver_membership

def run_membership_job() -> None:  # verb function
    silver_membership_df = build_silver_membership()
    # write via ManagedTableClient, emit metrics, etc.

if __name__ == "__main__":
    run_membership_job()
```

```python
# transforms/membership_transform.py  (noun file; houses verb functions)
from typing import Iterable
from pyspark.sql import DataFrame
from dataplatform.client.managed_table_client import ManagedTableClient
from pyspark.sql.functions import col, max as max_, min as min_, coalesce
from dataplatform.client.managed_table_client import ManagedTableClient
from dataplatform.core.argparser import get_arguments, is_argument
from dataplatform.core.logger import get_logger

def build_silver_membership() -> DataFrame:
    source_df = read_table("bronze.member.membership")
    staged_df = _sanitize_columns(source_df)
    deduped_df = _dedupe_membership(staged_df)
    assert_valid_dates(deduped_df, ["start_date", "end_date"])  # small extract
    return deduped_df


def _sanitize_columns(source_df: DataFrame) -> DataFrame:
    return source_df.select([col(c).alias(c.lower()) for c in source_df.columns])


def _dedupe_membership(source_df: DataFrame) -> DataFrame:
    window_cols: Iterable[str] = ["member_id", "club_id"]
    # implement deterministic dedupe based on latest business timestamp
    # ...
    return source_df
```

---

## 5) QA: One YAML per Table (Silver, Gold, Feature)
**Mandate:** Every **silver**, **gold**, and **feature_table** must ship with exactly **one QA YAML** capturing its data quality rules.

### 5.1 Location & Naming
- Directory: `dataplatform/<domain>/config/qa/`
- File name pattern: `<layer>_<table>_qa_task.yml` (e.g., `silver_membership_qa_task.yml`, `gold_member_kpis_qa_task.yml`, `feature_member_embedding_qa_task.yml`).

### 5.2 Minimum Required Checks
- **Schema**: expected columns & types.
- **Primary/business keys**: uniqueness and not‑null.
- **Nullability**: required columns not null (`member_id`, `club_id`, dates…).
- **Value constraints**: enums (e.g., `account_status in {Active, Suspended, Inactive}`).
- **Date quality**: not before `2000‑01‑01`; `start_date <= end_date`.
- **Row‑count bounds**: non‑zero, reasonable upper bounds, change deltas.

### 5.3 YAML Template (DQX‑style)
```yaml
- name: has_required_columns
  check:
  function: expect_schema
    arguments:
      columns:
        member_id: long
        club_id: long
        start_date: date
        end_date: date
- name: member_id_not_null
  check:
    function: not_null
    arguments:
      column: member_id
- name: unique_business_key
  check:
    function: unique_combination_of_columns
    arguments:
      columns: [member_id, club_id, start_date]
- name: status_in_list
  check:
    function: is_in_list
    arguments:
      column: account_status
      allowed: [Active, Suspended, Inactive]
- name: dates_valid
  check:
    function: expression_true_for_all
    arguments:
      expression: "start_date >= DATE('2000-01-01') and (end_date is null or end_date >= start_date)"
- name: reasonable_rowcount 
  check:
    function: row_count_between
    arguments:
      min: 1
      max: 100000000
```
> **Enforcement:** CI must fail if the table’s QA YAML is missing or any **blocking** check fails. Non‑blocking checks (warnings) should still be logged to monitoring tables and surfaced on dashboards.

---

## 6) Pull Requests & Reviews
- PR titles must match the ticket number: `feature/ds-123-build-member-table`
- Descriptions must explain the change intent so reviewer can understand what they are approving.
- Remove debug prints, commented code, unused variables/imports.
- Require at least one reviewer from data platform.

---

## 7) Databricks Jobs/Workflows
- Workflows are orchestration only. Business logic stays in `/transforms`.
- Job tasks:
  1) **Load** Bronze→Silver or Silver→Gold
  2) **Run QA** against target table (fail fast on blockers)
  3) **Publish** (e.g., refresh vector indexes, materialize aggregates)
- Use separate **dev/staging/prod** Jobs mapped to Unity Catalog catalogs.

---

## 8) Documentation & Discoverability
- Each transform module begins with a short docstring explaining inputs, outputs, and invariants.
- Each table has a one‑paragraph README in `docs/` linking to its QA YAML and lineage.
- Update the domain README when adding or renaming tables.

---

# Gold Table Standards

### 9.1 Table Naming
- Use singular nouns for table names.
- Prefix with business domain for clarity:
    - `gold.member.subscription`
    - `gold.pricing.subscription_revenue`

### 9.2 Date Fields
- Standardize all dates to `DATE` unless a true timestamp is required.
- If timestamp is available, include it in the model (`DATETIME` or `TIMESTAMP`).

### 9.3 Numeric Fields
- Apply consistent rounding:
    - Aggregations (avg, min, max) → round to nearest whole number (retain `.00`).
    - Financial metrics → round to 2 decimals.
    - Ratios / percentages → round to 4 decimals.
- Use numeric data types (`DECIMAL`) instead of strings.

### 9.4 Column Naming
- Use `snake_case`.
- Use business-friendly names (no cryptic codes).
    - ✅ `monthly_rate`
    - ✅ `personal_training_spend_first_30_days`
    - ❌ `mth_rt`

### 9.5 Aggregations
- Pre-compute aggregations where useful for reporting.
- Example metrics:
    - `total_sessions_used`
    - `avg_session_cost`
    - `subscription_revenue`

### 9.6 Data Quality & Governance
- All Gold tables must pass DQX / QA checks:
    - **Not Null** on primary keys.
    - **Valid Ranges** for numeric fields.
- Rows that fail validation should be quarantined with `_errors`.
- Note: currently `_errors` are logged but not separated out.

### 9.7 Documentation
- Each Gold table must have:
    - **README.md entry** explaining purpose, keys, and metrics.
    - **YAML schema** with QA rules.
    - Example queries for analysts.

---

## 10) Examples: Do / Don’t
**Tables**
- ✅ `silver.member.membership`
- ❌ `silver.member.memberships` (plural)
- ❌ `silver.member.member_pt` (abbreviation)

**Files**
- ✅ `membership_transform.py`
- ❌ `processMembership.py` (camelCase) / `pt_xform.py` (abbr.)

**Functions**
- ✅ `apply_scd2()`, `merge_into_delta()`, `compute_modified_date()`
- ❌ `scd2()` (unclear), `delta()` (noun), `doit()` (vague)

**Variables**
- ✅ `silver_member_df`, `pricing_calendar_df`
- ❌ `sm`, `pc`, `df` (generic), `pt_df` (abbr.)

---

## 11) Non‑Negotiables (Quick Checklist)
- [ ] Tables are **singular** and un‑abbreviated.
- [ ] Python **file names are nouns**; **functions are verbs**.
- [ ] DataFrame variables **end with `_df`**.
- [ ] No commented‑out code; remove noisy comments.
- [ ] Complex blocks **extracted** into smaller functions.
- [ ] `/task` thin; complexity lives in `/transforms`.
- [ ] **One QA YAML per** silver/gold/feature table.
- [ ] Avoid `F` alias; **import only needed** PySpark functions.
- [ ] Avoid `.collect()` on large data.
- [ ] CI runs QA and fails on blockers.
- [ ] **Remove unused code**.
- [ ] **Apply Rule of 3 for abstraction**.

