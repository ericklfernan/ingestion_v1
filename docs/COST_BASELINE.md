# Cost Baseline

> Nominal idle cost of running the file ingestion solution with zero files
> to process. Use this as the baseline for budgeting and optimization.
>
> Last updated: 2026-04-23

---

## Job Inventory

| Job | Job ID | Schedule | Trigger |
| --- | --- | --- | --- |
| Dispatcher | 665325945215561 | Every 5 min (`*/5 * * * *`) | PERIODIC (cron) |
| Ingestion | 410200508949307 | On-demand | ONE_TIME (triggered by dispatcher per feed) |

---

## Observed Run Durations (no payload, Apr 23 2026)

### Dispatcher (9 completed runs)

| Run time | Duration |
| --- | ---: |
| 14:45 | 51.4s |
| 14:40 | 55.5s |
| 14:35 | 51.4s |
| 14:30 | 71.0s |
| 14:25 | 51.4s |
| 14:20 | 50.1s |
| 14:15 | 59.2s |
| 14:10 | 54.7s |
| 14:05 | 51.0s |
| **Average** | **55s** |

### Ingestion (10 completed runs, 5 feeds x 2 hours)

| Feed | Duration |
| --- | ---: |
| co_mra | 41.3s |
| ep_mra | 36.5s |
| ep_aca | 38.2s |
| ci_mra | 36.5s |
| ci_aca | 39.6s |
| co_mra | 39.7s |
| ep_mra | 40.5s |
| ep_aca | 39.8s |
| ci_mra | 39.6s |
| ci_aca | 35.4s |
| **Average** | **39s** |

> Durations include serverless warm-up/spin-up overhead. Serverless compute
> is allocated at run start and released after completion — there is no
> persistent cluster. The startup cost is already embedded in the observed
> durations above.

---

## Billing Granularity

Databricks serverless jobs bill **per-second** based on actual compute
consumed. No documented minimum charge per run. The 55s and 39s durations
are billed as-is — no rounding penalty.

---

## Idle Cost Calculation

### Run Frequency

| Job | Runs/hour | Runs/day | Avg duration | Compute min/day |
| --- | ---: | ---: | ---: | ---: |
| Dispatcher | 12 | 288 | 55s | 264 |
| Ingestion | 5 | 120 | 39s | 78 |
| **Total** | **17** | **408** | | **342** |

### Cost at List Pricing

Assumptions: serverless jobs \~0.07 DBU/min, \~$0.07/DBU (list price).

| Period | Compute min | DBU | Cost |
| --- | ---: | ---: | ---: |
| Daily | 342 | 23.9 | $1.68 |
| Monthly (30d) | 10,260 | 718.2 | $50.27 |
| Yearly | 124,830 | 8,738.1 | $611.67 |

### Verify with Actual Billing Data

The estimate above uses approximate list rates. To get exact cost, an admin
with access to `system.billing.usage` can run:

```sql
SELECT
  usage_metadata.job_id,
  sku_name,
  usage_date,
  SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_metadata.job_id IN (665325945215561, 410200508949307)
  AND usage_date >= '2026-04-22'
GROUP BY ALL
ORDER BY usage_date DESC, job_id
```

---

## Cost Breakdown: What Happens in an Idle Run

### Dispatcher (55s avg)

| Phase | Est. time | What happens |
| --- | ---: | --- |
| Serverless spin-up | \~10-15s | Allocate compute, initialize Python/Spark |
| Config scan + sync | \~20-25s | Read CSV seeds, MERGE to config table |
| Schedule evaluation | \~5-10s | Evaluate sched_cron for each feed |
| Trigger ingestion runs | \~5-10s | run_now API call per eligible feed |
| **Total** | **\~55s** | |

### Ingestion per feed (39s avg)

| Phase | Est. time | What happens |
| --- | ---: | --- |
| Serverless spin-up | \~10-15s | Allocate compute, initialize Python/Spark |
| Request intake | \~15-20s | Build request, scan source dir, filter files |
| Check eligible files | \~5s | Condition task evaluates to false |
| Short circuit | 0s | No manifest, ingest, or finalize tasks run |
| **Total** | **\~39s** | |

> When no files are eligible, the ingestion job short-circuits after
> request_intake + check_eligible_files. The manifest, ingest, and finalize
> tasks do not execute.

---

## Optimization Levers

| Lever | Impact | Trade-off |
| --- | --- | --- |
| Increase dispatcher cron (e.g., `*/15`) | -67% dispatcher cost | Slower detection of new files |
| Reduce ingestion triggers (e.g., every 2h) | -50% ingestion cost | Longer lag to processing |
| Pause dispatcher during off-hours | -33% to -50% total cost | No overnight processing |
| Consolidate feeds (fewer feed_keys) | Linear reduction in ingestion runs | Less granular control |

### Example: 15-min dispatcher

| Period | Compute min | DBU | Cost | Savings |
| --- | ---: | ---: | ---: | ---: |
| Daily | 166 | 11.6 | $0.81 | -52% |
| Monthly | 4,980 | 348.6 | $24.40 | -52% |

---

## Scaling: Cost per Additional Feed

Each new feed adds one ingestion run per dispatcher cycle that triggers it.

| Metric | Per feed |
| --- | --- |
| Runs/day (hourly trigger) | 24 |
| Compute min/day | 15.6 |
| DBU/day | 1.1 |
| Cost/day | $0.08 |
| Cost/month | $2.33 |

---

## AI Assistant Cost Reference

| Metric | Value |
| --- | --- |
| Input tokens | \~$0.50 / 1M tokens |
| Output tokens | \~$1.50 / 1M tokens |
| Typical session (light) | 50-100K tokens \~ $0.03-$0.07 |
| Heavy session (file scans, doc gen) | 200-400K tokens \~ $0.15-$0.30 |
| Source of truth | Account Console -> Usage |
