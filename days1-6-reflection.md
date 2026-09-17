# Days 1-6 Reflection: Foundation Phase Complete

**Period:** Sept 11-16, 2026 | **Status:** Core pipeline rebuilt post-factory-reset ✅

---

## What Clicked

### 1. **Medallion Architecture is Intuitive**

Bronze → Silver → Gold feels natural now. Not just theory:
- **Bronze** (raw ingestion): commit-as-is, no transformation. Just partitioning + Delta format.
- **Silver** (dedup + quality gates): ROW_NUMBER window function makes the dedup logic elegant. Quarantine invalid rows (don't silently drop). This is how real pipelines should work.
- **Gold** (star schema): surrogate keys, fact tables with proper grain (campaign × date). Built twice (PySpark + dbt) to see both paradigms side-by-side.

The key insight: **each layer has a single, clear job.** You could explain this to a non-engineer and it makes sense.

### 2. **Idempotency is Not Magic—It's Boring but Correct**

`INSERT ... ON CONFLICT DO UPDATE` on (campaign_id, date). Rerun the pipeline 10 times, you still get the same Postgres row counts. No duplicates. This is the difference between a demo and a real production pipeline.

**Why it matters (for interviews):** shows you think about safety + re-runability, not just "does it work once?"

### 3. **Window Functions Are Powerful and Clear**

`ROW_NUMBER() OVER (PARTITION BY business_key ORDER BY ingestion_ts)` to keep the latest version per key—this is the actual pattern used in real data teams. Understood deeply now, not just syntax.

### 4. **Airflow Dependencies Are Strict and Predictable**

Bronze → Silver → Gold → Postgres → serving. If Bronze fails, nothing downstream runs. This is what orchestration *should* do. No surprise stale data.

### 5. **Two Implementations of Gold (PySpark vs. dbt) Show Real Trade-offs**

Running both deliberately highlights:
- PySpark: more flexible, you control every compute step, easier to debug with `.show()`.
- dbt: declarative SQL, source lineage, built-in testing framework, easier for SQL-first teams.

In a real interview, this demonstrates maturity: "Here's the trade-off; here's why we'd pick one."

---

## What's Still Fuzzy

### 1. **SCD Type 2 (Slowly Changing Dimensions) — Not Yet Explored**

The README says "Type-1-shaped for future extension," but I haven't implemented true Type 2 (keeping history with effective_date ranges). This is a gap. It'll come up in interviews.

**Next:** Days 14-16 should tackle this. Real merchant attributes (name, region, status) change over time; you need to track which versions were active when.

### 2. **Airflow Error Recovery & Retry Logic — Thin Understanding**

The DAG has dependencies, but I haven't tuned:
- **Retry counts & backoff strategies:** how many times should a task retry? Exponential backoff?
- **Alerts:** what triggers a Slack/email alert when something fails?
- **SLA monitoring:** the README mentions it, but no metrics dashboard yet.

**Why it matters:** Production Airflow is not "set it and forget it." It's about observability.

### 3. **dbt Testing — Skeletal**

dbt has powerful testing (unique constraints, relationships, custom SQL tests), but I've only scratched the surface. The gold layer should have rigorous tests:
- `unique(campaign_id, date)` on the fact table (grain).
- `not_null` on critical dimensions.
- Custom tests: "fact table should never have negative spend."

**Next:** Days 11-12 in the prep plan cover this deeply.

### 4. **Performance Tuning & Query Optimization**

Current Postgres queries are simple (`SELECT COUNT(*) FROM fact`). No experience yet with:
- Indexes on (merchant_id, date) for fast slicing.
- Explaining query plans (`EXPLAIN ANALYZE`).
- Partitioning strategies for billion-row fact tables.

**Why it matters:** "The pipeline is slow" is a real senior engineer problem. I need to think about this.

### 5. **Spark Performance Tuning**

No deep work yet on:
- Shuffles: which operations trigger expensive network I/O? (joins, groupBy)
- Partitioning strategy: how to partition the data for parallel reads?
- Caching: when to cache intermediate results vs. let Spark optimize?

**Why it matters:** "Scale to 10B rows" is a common interview question.

---

## Confidence Assessment

| Skill | Confidence | Evidence | Notes |
|-------|-----------|----------|-------|
| **Medallion architecture** | 8/10 | Built it end-to-end with data quality gates. Can explain to anyone. | Would be 10/10 if I'd also built incremental/merge-on-read variants |
| **Star schema design** | 7/10 | Surrogate keys, fact grain, dimensions. Built it in both PySpark + dbt. | Fuzzy on slowly changing dimensions (SCD Type 2) |
| **Window functions (SQL)** | 8/10 | Dedup logic via ROW_NUMBER is clear. Understand PARTITION BY + ORDER BY. | Haven't used LAG/LEAD yet; haven't tackled complex frames |
| **Idempotent pipeline design** | 8/10 | Upserts work. Reruns are safe. No accidental duplicates. | Haven't dealt with late-arriving facts or backfilling scenarios |
| **Airflow orchestration** | 6/10 | DAG runs end-to-end. Task dependencies work. | Weak on error recovery, retries, SLA alerting, dynamic task generation |
| **dbt** | 6/10 | Models run. Can read dbt code. Built Gold layer in dbt. | Weak on tests, macros, hooks, jinja templating, performance |
| **PySpark (transformation)** | 7/10 | Can read + write DataFrames. Understand lazy evaluation. Built Gold layer in PySpark too. | Weak on query optimization, shuffle behavior, catalyst plans |
| **Postgres (serving)** | 7/10 | Can insert/update/upsert. Built two loading methods (via driver, via Spark JDBC). | Weak on indexes, query optimization, partitioning strategies |
| **Python (general)** | 9/10 | 15 years backend/data eng. Can write clean production code. | Not a gap |

**Overall Confidence: 7.3/10**

---

## Key Questions Answered

### Can you explain medallion architecture?
**Yes.** Bronze = raw staging (partition early, don't transform). Silver = deduplicated + validated (window functions + quarantine invalid). Gold = modeled for analytics (star schema, surrounded by dbt tests). Data quality gates at each layer, not silent drops.

### Can you trace data from CSV → Delta → Postgres?
**Yes.** 
1. `day01_bronze_ingestion.py` reads CSV, writes partitioned Delta table (500 rows).
2. `day02_silver_transformation.py` deduplicates via ROW_NUMBER window function, writes to delta_silver (250 rows).
3. `day03_gold_layer.py` + dbt build star schema (fact + dimensions).
4. `day05_postgres_load.py` upserts to Neon (idempotent ON CONFLICT).
5. Airflow DAG orchestrates all five in order.

### Can you write a window function?
**Yes.** 
```sql
ROW_NUMBER() OVER (PARTITION BY campaign_id, merchant_id ORDER BY ingestion_ts DESC) AS rn
WHERE rn = 1  -- keeps latest occurrence per business key
```
Also understand LAG/LEAD conceptually (haven't used yet).

### Can you design for idempotency?
**Yes.** Think about the upsert key (what makes a row unique?), use `INSERT ... ON CONFLICT DO UPDATE`, test by rerunning and verifying row counts stay the same.

---

## What This Phase Accomplished

✅ Rebuilt entire pipeline from scratch (post-factory-reset, same as "starting Day 1 cold").
✅ Validated medallion architecture in practice (not just reading about it).
✅ Implemented data quality gates (schema validation, quarantine table).
✅ Designed star schema with surrogate keys.
✅ Built idempotent serving layer (Postgres).
✅ Orchestrated end-to-end with Airflow.
✅ Documented trade-offs openly (not hiding design decisions).

---

## Priorities for Next Week (Days 7-13)

Based on gaps above, in order of value:

1. **SQL + Window Functions Deep Dive (Days 7-9):**
   - LAG/LEAD for time-series (month-over-month deltas).
   - DENSE_RANK vs ROW_NUMBER vs RANK (understand all three).
   - Cumulative sums (running totals).
   - Goal: solve 10-15 SQL problems on LeetCode/Mode.

2. **Python Algorithms Interview Gym (Days 7-12):**
   - Arrays, hashing, two-pointer patterns.
   - Build up to 15-20 problems passing.
   - Focus on clarity + correctness (not fancy).

3. **Airflow Error Recovery & Monitoring (Days 13-15):**
   - Retry logic, backoff strategies, SLA alerts.
   - Add logging + alerts to the DAG.

4. **dbt Testing Framework (Days 11-12):**
   - Write tests for the gold layer (unique, not_null, relationships, custom).
   - Understand macro patterns for reusable test logic.

5. **PySpark Optimization (Days 16-18):**
   - Query plans (EXPLAIN).
   - Shuffle operations (joins, groupBy).
   - Partitioning strategy.

---

## Reflection: How Is This Prep Different?

**This is not a tutorial project.** It's a real, production-grade pipeline that:
- Handles data quality (quarantine, not silent drops).
- Implements two competing approaches (PySpark vs. dbt) for direct comparison.
- Documents design trade-offs openly.
- Uses real tools (Databricks, Delta, dbt, Airflow, Postgres).
- Runs end-to-end, orchestrated, idempotent.

**In an interview**, this is the kind of project you want to point to. "I didn't just read about medallion architecture; I built it, debugged the Databricks platform constraints, and ran it in production (local Docker)."

---

## Next: Phase 2 Kickoff (Day 7)

Ready to move into intensive phase: SQL deep dive + Python algorithms + real system design scenarios.

**Current date:** Sept 16, 2026 | **Target interview:** late October 2026 | **Days left:** 24
