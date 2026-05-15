# Data Quality Design Specification

> **Status:** Analysis complete — implementation pending.
> **Created:** 2026-05-05
> **Scope:** `pipelines/file_ingestion/validate/` + supporting framework changes

---

## 1. Design Principles

| # | Principle |
| --- | --- |
| 1 | **Opt-in per feed_key** — no rules configured = zero pipeline impact |
| 2 | **Per-feed rules only** — no wildcard/universal rules, granular and auditable |
| 3 | **Greenfield** — no backward compatibility |
| 4 | **Databricks DLT alignment** — mirrors Expectations model (EXPECT, DROP ROW, FAIL UPDATE) |
| 5 | **SQL expressions** — same syntax as DLT constraints, evaluated via `F.expr()` |
| 6 | **Separate config surface** — `seeds/dq_rules/dq_rules.csv` (not in ingestion config) |
| 7 | **Inline evaluation** — inside `write_to_bronze.py`, between read and write |

---

## 2. Actions (DLT Alignment)

| Our Action | DLT Equivalent | Behavior |
| --- | --- | --- |
| `WARN` | `EXPECT` | Write all rows to bronze, log violation count |
| `DROP` | `EXPECT ON VIOLATION DROP ROW` | Filter out failing rows before write, log dropped count |
| `FAIL` | `EXPECT ON VIOLATION FAIL UPDATE` | Abort entire file, mark DQ_FAILED in inventory |
| `QUARANTINE` | Quarantine pattern (from DLT docs) | Write failing rows to quarantine table, remainder to bronze |

---

## 3. Rules CSV Schema

**Location:** `seeds/dq_rules/dq_rules.csv`

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `feed_key` | STRING | No | Which feed (per-feed only, no wildcards) |
| `rule_name` | STRING | No | Unique within feed (e.g., `member_dob_castable`) |
| `rule_tier` | INT | No | Evaluation priority: 1=critical gate, 2=standard, 3=soft |
| `depends_on` | STRING | Yes | Pipe-separated prerequisite rule_names. NULL=independent. AND semantics |
| `rule_type` | STRING | No | Category: `COMPLETENESS` / `VALIDITY` / `TIMELINESS` / `ACCURACY` |
| `rule_expression` | STRING | No | SQL boolean expression evaluated via `F.expr()` |
| `action_on_failure` | STRING | No | `WARN` / `DROP` / `FAIL` / `QUARANTINE` |
| `threshold_pct` | FLOAT | Yes | Max tolerable failure %. NULL or empty = 0 (zero tolerance) |
| `is_active` | STRING | No | `Y` = evaluate, `N` = skip |

**Primary key:** (`feed_key`, `rule_name`)

---

## 4. Rule Categories

Categories are for **reporting and classification only** — they do not affect evaluation behavior.

| Category | Definition | What it answers | Expression patterns |
| --- | --- | --- | --- |
| `COMPLETENESS` | Required data is present | "Is the data here?" | `IS NOT NULL`, composite NOT NULL, at-least-one-of |
| `VALIDITY` | Conforms to expected format/type/values | "Is it well-formed?" | `CAST AS DATE IS NOT NULL`, `RLIKE`, `IN (...)`, `LENGTH` |
| `TIMELINESS` | Falls within acceptable recency | "Is it fresh enough?" | `>= date_sub(current_date(), N)`, `<= current_date()` |
| `ACCURACY` | Values within logical/business bounds | "Does it make sense?" | `BETWEEN`, cross-column logic, derived checks |

### Expression Pattern Examples

**COMPLETENESS:**
```sql
-- Single NOT NULL
MemberID IS NOT NULL

-- Composite NOT NULL (key columns)
MEM_CARD_ID IS NOT NULL AND MemberID IS NOT NULL AND ClientChartID IS NOT NULL

-- At least one of N
MemberHIC IS NOT NULL OR MBI IS NOT NULL

-- Not blank/whitespace
TRIM(MemberLastName) != ''
```

**VALIDITY:**
```sql
-- ENUM (allowed values)
MemberGender IN ('M','F','U')

-- REGEX format (NPI = 10 digits)
NPI IS NULL OR NPI RLIKE '^[0-9]{10}$'

-- Type castable (string → DATE)
CAST(MemberDOB AS DATE) IS NOT NULL

-- Boolean flag
ON_HOLD IN ('Y','N')

-- Length check
LENGTH(MemberSSNLastFour) = 4

-- State code
OriginalState IS NULL OR OriginalState RLIKE '^[A-Z]{2}$'
```

**TIMELINESS:**
```sql
-- Date recency (within last year)
CodingDate IS NULL OR CAST(CodingDate AS DATE) >= date_sub(current_date(), 365)

-- Not future date
CodingDate IS NULL OR CAST(CodingDate AS DATE) <= current_date()

-- Eligibility window
CAST(Eligibility_End AS DATE) >= date_sub(current_date(), 90)
```

**ACCURACY:**
```sql
-- Numeric range
CAST(HCCCount AS INT) BETWEEN 0 AND 100

-- Non-negative
CAST(InvoiceAmount AS DOUBLE) >= 0

-- Cross-column (New <= Total)
HCCCount IS NULL OR HCCNewCount IS NULL OR CAST(HCCNewCount AS INT) <= CAST(HCCCount AS INT)

-- DOB sanity (age 0-120 years)
DATEDIFF(current_date(), CAST(MemberDOB AS DATE)) BETWEEN 0 AND 43800
```

**Note:** `IS NULL OR <check>` pattern = nullable columns where NULL is acceptable but wrong values are not.

---

## 5. Tier Waterfall Model

```
TIER 1 (CRITICAL GATE — any FAIL action triggered = abort file, skip tier 2+)
┌──────────────────────────────────────────────────────────────────────┐
│  key_cols_complete ←── independent                                   │
│  identifier_present ←── independent                                  │
│  member_name_present ←── independent                                 │
│                                                                      │
│  IF ANY rule with action=FAIL has pct_fail > threshold → ABORT FILE  │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (all tier 1 passed)
TIER 2 (STRUCTURAL — DROP bad rows, WARN on issues)
┌──────────────────────────────────────────────────────────────────────┐
│  member_dob_present ←── independent                                  │
│       │                                                              │
│       ▼                                                              │
│  member_dob_castable ←── depends_on: member_dob_present              │
│       │                                                              │
│       ▼                                                              │
│  member_dob_sane ←── depends_on: member_dob_present|member_dob_castable│
│                                                                      │
│  gender_valid ←── independent (parallel)                             │
│  npi_format ←── independent (parallel)                               │
│  coding_date_castable ←── independent                                │
│  hcc_count_numeric ←── independent                                   │
│  on_hold_flag ←── independent                                        │
│                                                                      │
│  IF ANY FAIL action triggered → ABORT FILE, skip tier 3              │
│  DROP rules → filter DataFrame (reduced row count for tier 3)        │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼ (all tier 2 FAIL rules passed)
TIER 3 (SOFT — WARN only, never blocks pipeline)
┌──────────────────────────────────────────────────────────────────────┐
│  coding_date_recent ←── depends_on: coding_date_castable             │
│  coding_date_not_future ←── depends_on: coding_date_castable         │
│  hcc_count_range ←── depends_on: hcc_count_numeric                   │
│  hcc_new_lte_total ←── depends_on: hcc_count_numeric                 │
│  invoice_non_negative ←── independent                                │
│                                                                      │
│  All WARN — logged for reporting, never blocks pipeline              │
└──────────────────────────────────────────────────────────────────────┘
```

**Rules within a tier:** Independent by default. Only gated if `depends_on` is specified.

---

## 6. Dependency Model

### Semantics

| `depends_on` value | Behavior |
| --- | --- |
| NULL / empty | Independent — always evaluates within its tier |
| `rule_a` | Only evaluates if `rule_a` outcome is `PASSED` or `WARNED` |
| `rule_a\|rule_b\|rule_c` | Only evaluates if ALL listed rules are `PASSED` or `WARNED` (AND logic) |

### Evaluation when dependency not met

If ANY prerequisite has outcome `FAILED`, `DROPPED`, `SKIPPED`, or `QUARANTINED`:
- This rule is marked `SKIPPED`
- `skip_reason` records which dependency failed
- Downstream rules depending on this one are also `SKIPPED` (cascade)

### Circular dependency detection

At rule load time: topological sort within each tier. If cycle detected → both rules marked `SKIPPED` with error logged to ops_notifications.

### Evaluation order within a tier

1. Sort by dependency depth (no deps first, then depth 1, then depth 2...)
2. Evaluate each rule in order
3. Check `depends_on` before evaluation — if unmet, mark SKIPPED immediately

### Example chain

```
member_dob_present (tier 2, no deps)
    └──→ member_dob_castable (tier 2, depends_on: member_dob_present)
              └──→ member_dob_sane (tier 2, depends_on: member_dob_present|member_dob_castable)
                        └──→ member_age_range (tier 3, depends_on: member_dob_present|member_dob_castable|member_dob_sane)
```

If `member_dob_present` fails → `member_dob_castable` SKIPPED → `member_dob_sane` SKIPPED → `member_age_range` SKIPPED.

---

## 7. Threshold Mechanism

| `threshold_pct` | `pct_fail` | `threshold_breached` | Result |
| --- | --- | --- | --- |
| NULL (= 0) | 0.1% | `Y` | Action triggered (zero tolerance) |
| 5 | 3.2% | `N` | PASSED (within tolerance) |
| 5 | 7.8% | `Y` | Action triggered (exceeds 5%) |
| 10 | 10.0% | `N` | PASSED (at boundary, not exceeded) |
| 10 | 10.1% | `Y` | Action triggered |

**Key insight:** `threshold_pct` eliminates the need for separate `ROW` vs `FILE` scope columns. The threshold IS the file-level logic:
- `threshold_pct = 0` + `action = FAIL` → "any single violation fails the file"
- `threshold_pct = 5` + `action = WARN` → "tolerate up to 5%, warn above"
- `threshold_pct = 10` + `action = DROP` → "drop rows only if >10% fail"

---

## 8. Outcome Logic

| Condition | Outcome |
| --- | --- |
| Dependency not met | `SKIPPED` |
| `pct_fail <= threshold_pct` | `PASSED` |
| `pct_fail > threshold_pct` AND action = `WARN` | `WARNED` |
| `pct_fail > threshold_pct` AND action = `DROP` | `DROPPED` |
| `pct_fail > threshold_pct` AND action = `FAIL` | `FAILED` |
| `pct_fail > threshold_pct` AND action = `QUARANTINE` | `QUARANTINED` |

---

## 9. New Tables (4)

### 9.1 ops_cfg_dq_rules

Synced from `seeds/dq_rules/dq_rules.csv` via scan_config. Mechanism: **OVERWRITE** (full replace, not MERGE). CSV always wins.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `feed_key` | STRING | No | Feed this rule applies to |
| `rule_name` | STRING | No | Unique identifier within feed |
| `rule_tier` | INT | No | Evaluation priority tier |
| `depends_on` | STRING | Yes | Pipe-separated prerequisite rule_names |
| `rule_type` | STRING | No | COMPLETENESS/VALIDITY/TIMELINESS/ACCURACY |
| `rule_expression` | STRING | No | SQL boolean expression |
| `action_on_failure` | STRING | No | WARN/DROP/FAIL/QUARANTINE |
| `threshold_pct` | DOUBLE | Yes | Max tolerable failure % |
| `is_active` | STRING | No | Y/N |
| `config_source_file` | STRING | Yes | CSV filename (audit trail) |
| `ts_synced` | TIMESTAMP | No | When synced from CSV |

### 9.2 ops_dq_results

One row per rule per file evaluation.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `event_id` | STRING | No | UUID |
| `feed_key` | STRING | No | Feed |
| `file_name` | STRING | No | Source file evaluated |
| `file_fingerprint` | STRING | No | Links to ops_file_inventory |
| `rule_name` | STRING | No | Rule evaluated |
| `rule_tier` | INT | No | Tier of this rule |
| `rule_type` | STRING | No | Category |
| `rule_expression` | STRING | No | The SQL check applied |
| `action_on_failure` | STRING | No | Configured action |
| `cnt_total` | BIGINT | No | Total rows evaluated |
| `cnt_pass` | BIGINT | No | Rows passing |
| `cnt_fail` | BIGINT | No | Rows failing |
| `pct_fail` | DOUBLE | No | Failure percentage (0.0–100.0) |
| `threshold_pct` | DOUBLE | Yes | Configured threshold |
| `threshold_breached` | STRING | No | Y/N — was threshold exceeded? |
| `outcome` | STRING | No | PASSED/WARNED/DROPPED/FAILED/QUARANTINED/SKIPPED |
| `skip_reason` | STRING | Yes | If SKIPPED: which dependency failed |
| `request_id` | STRING | Yes | Traceability |
| `dispatch_run_id` | STRING | Yes | Traceability |
| `ts_evaluated` | TIMESTAMP | No | When evaluated |

### 9.3 {feed_key}_quarantine (per-feed)

Same pattern as bronze tables. Full row data preserved. Created lazily on first quarantine event.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `quarantine_id` | STRING | No | UUID — FK to ops_dq_violations |
| `src_row_number` | BIGINT | No | 1-based row position in source file |
| *(all business columns)* | STRING | Yes | Full source row preserved exactly as-is |
| `feed_key` | STRING | No | Feed |
| `file_name` | STRING | No | Source file |
| `file_fingerprint` | STRING | No | Traceability to inventory |
| `request_id` | STRING | Yes | Traceability |
| `dispatch_run_id` | STRING | Yes | Traceability |
| `ts_quarantined` | TIMESTAMP | No | When quarantined |

**Notes:**
- Business columns are dynamic (varies per feed, same as bronze)
- Table naming: `{catalog}.{schema}.{feed_key}_quarantine`
- `src_row_number`: 1-based, matches source file line position (header offset accounted for)

### 9.4 ops_dq_violations (shared across feeds)

One row per violation per quarantined row. Compact — no business columns duplicated.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `violation_id` | STRING | No | UUID |
| `quarantine_id` | STRING | No | FK to `{feed_key}_quarantine` |
| `feed_key` | STRING | No | Feed |
| `file_name` | STRING | No | Source file |
| `file_fingerprint` | STRING | No | Traceability |
| `src_row_number` | BIGINT | No | Row position in source file |
| `rule_name` | STRING | No | Which rule was violated |
| `rule_tier` | INT | No | Tier of violated rule |
| `rule_type` | STRING | No | Category |
| `rule_expression` | STRING | No | The check that failed |
| `columns_violated` | STRING | Yes | Pipe-separated column names involved |
| `column_values` | STRING | Yes | Pipe-separated actual values (truncated 200 chars each) |
| `ts_evaluated` | TIMESTAMP | No | When detected |

**Join pattern:**
```sql
SELECT v.rule_name, v.columns_violated, q.*
FROM ops_dq_violations v
JOIN {feed_key}_quarantine q ON v.quarantine_id = q.quarantine_id
WHERE v.file_name = '...'
```

---

## 10. Modified Tables

### ops_file_inventory

| New Column | Type | Default | Description |
| --- | --- | --- | --- |
| `dq_status` | STRING | `NOT_EVALUATED` | DQ outcome for this file |

**Allowed values:** `NOT_EVALUATED`, `PASSED`, `WARNED`, `FAILED`, `QUARANTINED`

**Impact on finalize adjudication:**
```
Current:  load_status = 'LOADED_BRONZE' → READY_FOR_SILVER

With DQ:  load_status = 'LOADED_BRONZE'
          AND dq_status IN ('NOT_EVALUATED', 'PASSED', 'WARNED')
          → READY_FOR_SILVER
```

`NOT_EVALUATED` ensures feeds without DQ rules promote normally (opt-in principle).

---

## 11. Evaluation Flow (inside write_to_bronze.py)

```python
# ─── Existing: read source file ───
df_raw = spark.read.csv(...)

# ─── NEW: DQ Evaluation (~25 lines) ───
from pipelines.file_ingestion.validate.evaluate_dq import evaluate_dq, load_rules

rules = load_rules(spark, feed_key)
if rules:
    # Add src_row_number for quarantine traceability
    df_raw = df_raw.withColumn("_src_row_number", F.monotonically_increasing_id() + 1)

    for tier in sorted(unique_tiers):
        tier_rules = [r for r in rules if r["rule_tier"] == tier]
        # Sort by dependency depth (no deps first)
        tier_results = evaluate(df_raw, tier_rules, previous_outcomes)
        log_dq_results(spark, tier_results, ...)

        if any_fail_triggered(tier_results):
            mark_inventory_dq_failed(spark, file_fingerprint, tier, first_failure)
            emit_notification(CRITICAL, DQ_FILE_FAILED, ...)
            continue  # skip to next file in batch

        # Apply DROP filters
        df_raw = apply_drops(df_raw, tier_results)

        # Apply QUARANTINE (write failing rows to quarantine table)
        apply_quarantines(df_raw, tier_results, ...)

    # Drop helper column before bronze write
    df_raw = df_raw.drop("_src_row_number")
    mark_inventory_dq_status(spark, file_fingerprint, overall_outcome)

# ─── Existing continues: enrich + write to bronze ───
df_bronze = df_raw.withColumn(...)
writer.saveAsTable(...)
```

**Key behavior:** If `rules` is empty (no DQ config for this feed), the entire DQ block is skipped — zero overhead.

---

## 12. Config Sync Pattern

| Aspect | Ingestion Config | DQ Rules |
| --- | --- | --- |
| Source of truth | `seeds/config/*.csv` | `seeds/dq_rules/dq_rules.csv` |
| Delta table | `ops_cfg_file_ingestion` | `ops_cfg_dq_rules` |
| Sync mechanism | MERGE (upsert by feed_key) | **OVERWRITE** (full replace) |
| State columns needed | Yes (ctl_sync_config, ctl_active) | No — read-only, CSV always wins |
| When synced | scan_config at dispatcher start | Same scan_config step (one extra load) |

Simpler than ingestion config: no merge keys, no sync flags, no state management.

---

## 13. Integration with Notifications

DQ events emit to `ops_notifications` (same pattern as schema drift):

| DQ Outcome | Severity | Event Type |
| --- | --- | --- |
| WARNED | `WARNING` | `DQ_THRESHOLD_BREACHED` |
| DROPPED | `WARNING` | `DQ_ROWS_DROPPED` |
| FAILED | `CRITICAL` | `DQ_FILE_FAILED` |
| QUARANTINED | `WARNING` | `DQ_ROWS_QUARANTINED` |

---

## 14. Sample Rules CSV

```csv
feed_key,rule_name,rule_tier,depends_on,rule_type,rule_expression,action_on_failure,threshold_pct,is_active
retro_status_report_ci_aca,key_cols_complete,1,,COMPLETENESS,"MEM_CARD_ID IS NOT NULL AND MemberID IS NOT NULL AND ClientChartID IS NOT NULL",FAIL,,Y
retro_status_report_ci_aca,identifier_present,1,,COMPLETENESS,"MemberHIC IS NOT NULL OR MBI IS NOT NULL",FAIL,1,Y
retro_status_report_ci_aca,member_name_present,1,,COMPLETENESS,"MemberFirstName IS NOT NULL AND MemberLastName IS NOT NULL",FAIL,2,Y
retro_status_report_ci_aca,member_dob_present,2,,COMPLETENESS,"MemberDOB IS NOT NULL",DROP,5,Y
retro_status_report_ci_aca,member_dob_castable,2,member_dob_present,VALIDITY,"CAST(MemberDOB AS DATE) IS NOT NULL",DROP,10,Y
retro_status_report_ci_aca,member_dob_sane,2,member_dob_present|member_dob_castable,ACCURACY,"DATEDIFF(current_date(), CAST(MemberDOB AS DATE)) BETWEEN 0 AND 43800",WARN,5,Y
retro_status_report_ci_aca,gender_valid,2,,VALIDITY,"MemberGender IN ('M','F','U')",DROP,5,Y
retro_status_report_ci_aca,npi_format,2,,VALIDITY,"NPI IS NULL OR NPI RLIKE '^[0-9]{10}$'",WARN,10,Y
retro_status_report_ci_aca,state_code_valid,2,,VALIDITY,"OriginalState IS NULL OR OriginalState RLIKE '^[A-Z]{2}$'",WARN,15,Y
retro_status_report_ci_aca,coding_date_castable,2,,VALIDITY,"CodingDate IS NULL OR CAST(CodingDate AS DATE) IS NOT NULL",WARN,10,Y
retro_status_report_ci_aca,hcc_count_numeric,2,,VALIDITY,"HCCCount IS NULL OR CAST(HCCCount AS INT) IS NOT NULL",WARN,5,Y
retro_status_report_ci_aca,on_hold_flag,2,,VALIDITY,"ON_HOLD IN ('Y','N')",DROP,2,Y
retro_status_report_ci_aca,on_off_ind_valid,2,,VALIDITY,"ON_OFF_IND IN ('ON','OFF')",DROP,2,Y
retro_status_report_ci_aca,coding_date_recent,3,coding_date_castable,TIMELINESS,"CodingDate IS NULL OR CAST(CodingDate AS DATE) >= date_sub(current_date(), 730)",WARN,20,Y
retro_status_report_ci_aca,coding_date_not_future,3,coding_date_castable,TIMELINESS,"CodingDate IS NULL OR CAST(CodingDate AS DATE) <= current_date()",WARN,5,Y
retro_status_report_ci_aca,hcc_count_range,3,hcc_count_numeric,ACCURACY,"HCCCount IS NULL OR CAST(HCCCount AS INT) BETWEEN 0 AND 100",WARN,5,Y
retro_status_report_ci_aca,hcc_new_lte_total,3,hcc_count_numeric,ACCURACY,"HCCCount IS NULL OR HCCNewCount IS NULL OR CAST(HCCNewCount AS INT) <= CAST(HCCCount AS INT)",WARN,5,Y
retro_status_report_ci_aca,invoice_non_negative,3,,ACCURACY,"InvoiceAmount IS NULL OR CAST(InvoiceAmount AS DOUBLE) >= 0",WARN,5,Y
```

---

## 15. Quarantine Design (Option C: Two Tables)

**Decision:** Two-table design — no data duplication, both row-level and rule-level queries are simple.

| Table | Scope | Content |
| --- | --- | --- |
| `{feed_key}_quarantine` | Per-feed | Full row data + `src_row_number` + `quarantine_id` |
| `ops_dq_violations` | Shared | Rule-level detail (compact, no business columns) |

**src_row_number:** Assigned at read time via `monotonically_increasing_id()` + `row_number()` window. 1-based, matches source file line number (accounting for header).

**Query patterns:**
```sql
-- All quarantined rows for a file
SELECT * FROM {feed_key}_quarantine WHERE file_fingerprint = '...'

-- Violations for a specific row
SELECT * FROM ops_dq_violations WHERE quarantine_id = '...'

-- Top failing rules across all feeds
SELECT rule_name, feed_key, COUNT(*) AS cnt
FROM ops_dq_violations
GROUP BY rule_name, feed_key
ORDER BY cnt DESC

-- Full picture (row + violations joined)
SELECT v.rule_name, v.columns_violated, q.*
FROM ops_dq_violations v
JOIN {feed_key}_quarantine q ON v.quarantine_id = q.quarantine_id
WHERE v.file_name = '...'
```

---

## 16. File Structure

### New files

```
seeds/dq_rules/
  └── dq_rules.csv                              ← rules for all feeds

pipelines/file_ingestion/validate/              ← new module
  ├── __init__.py
  └── evaluate_dq.py                            ← engine (~200 lines)

src/framework/
  ├── schemas.py                                ← + dq_results_schema(), dq_violations_schema()
  └── tracking/
      ├── table_names.py                        ← + dq table name functions
      └── ddl.py                                ← + CREATE TABLE DDL

docs/
  ├── DATA_QUALITY.md                           ← main user reference
  └── DQ_RULES_REFERENCE.md                     ← cookbook/examples

tests/unit/
  └── test_evaluate_dq.py                       ← unit tests
```

### Modified existing files

| File | Change |
| --- | --- |
| `pipelines/file_ingestion/ingest/write_to_bronze.py` | ~25 lines: load rules → evaluate → act |
| `pipelines/file_ingestion/finalize/close_and_summarize.py` | Add `dq_status` to adjudication WHERE clause |
| `pipelines/file_ingestion/file_ingestion_pipeline.py` | Update docstring (add VALIDATE step) |
| `src/framework/tracking/ddl.py` | Add ops_dq_results, ops_dq_violations DDL |
| `src/framework/tracking/table_names.py` | Register new table name functions |
| `src/framework/schemas.py` | Add dq_results_schema(), dq_violations_schema() |

---

## 17. Key Design Decisions

| # | Decision | Rationale |
| --- | --- | --- |
| 1 | Inline (not separate job) | FAIL must abort before write; single pass efficiency |
| 2 | Per-feed only (no wildcard `*`) | Granular, auditable, no hidden inherited behavior |
| 3 | Opt-in (no rules = no impact) | `dq_status` defaults to `NOT_EVALUATED`, promotes normally |
| 4 | SQL expressions via `F.expr()` | Same as DLT, portable, testable |
| 5 | Threshold as file-level mechanism | Eliminates need for separate ROW/FILE scope column |
| 6 | `depends_on` with pipe AND logic | Supports multi-dependency chains, simple lookup |
| 7 | Tier waterfall | Gates between tiers; independent within (unless depends_on) |
| 8 | Two-table quarantine (Option C) | No data duplication, compact violations, scalable |
| 9 | OVERWRITE sync (not MERGE) | CSV always wins, no state management needed |
| 10 | `src_row_number` | Traceable back to exact source file line |

---

## 18. What Is NOT In Scope

| Excluded | Reason |
| --- | --- |
| Separate DQ job | Inline is simpler and catches FAIL before write |
| DQ dashboard | Just the results table for now (dashboard later) |
| ML-based anomaly detection | Declarative rules only |
| Rule versioning system | CSV is versioned in git (sufficient) |
| Complex DAG between rules | Simple linear `depends_on` chain (no graph engine) |
| Universal/wildcard rules | Explicit per-feed only |
| OR logic in depends_on | AND-only; use tier restructuring for OR-like behavior |

---

## 19. Resume Instructions

To resume implementation, say:

> "Let's resume the DQ implementation from docs/DQ_DESIGN_SPEC.md"

Implementation order:
1. Create `seeds/dq_rules/dq_rules.csv` with sample rules
2. Create `pipelines/file_ingestion/validate/evaluate_dq.py` (engine)
3. Update `src/framework/` (schemas, table_names, ddl)
4. Update `write_to_bronze.py` (inline integration)
5. Update `close_and_summarize.py` (dq_status in adjudication)
6. Create `tests/unit/test_evaluate_dq.py`
7. Create `docs/DATA_QUALITY.md` + `docs/DQ_RULES_REFERENCE.md`
