# Cron & Quartz Expression Reference

> **Unified format: Quartz 6-field** — as of 2026-05-04, the file-ingestion framework uses Quartz 6-field
> cron expressions everywhere: `ops_cfg_file_ingestion.sched_cron`, Databricks Job YAML, and the custom
> `evaluate_schedule.py` parser. The Standard 5-field column is retained below for external reference only
> (Linux crontab, third-party tools).

## Format Overview

| Aspect | Quartz 6-field (framework standard) | Standard 5-field (external reference) |
| --- | --- | --- |
| Field order | `sec min hour dom month dow` | `min hour dom month dow` |
| Day-of-week values | `1-7` (1 = Sun) or `SUN-SAT` | `0-7` (0 and 7 = Sun) |
| `?` wildcard | Required for `dom` OR `dow` (not both) | Not supported |
| `L` (last day) | Supported in `dom` field | Not supported |
| `W` (nearest weekday) | Supported in `dom` field | Not supported |
| `#` (Nth weekday) | Supported in `dow` field (e.g., `TUE#2`) | Not supported |
| `NL` (last weekday) | Supported in `dow` field (e.g., `6L` = last Friday) | Not supported |
| `LW` (last weekday of month) | Supported in `dom` field | Not supported |
| `L-N` (Nth before last) | Supported in `dom` field (e.g., `L-3`) | Not supported |
| `DOW/N` (week-step) | Supported in `dow` field (e.g., `MON/2`) | Not supported |
| Pipe `\|` (multi-cron) | Supported — earliest next-fire wins | Not supported |
| Timezone (`tz_name`) | Supported via `sched_timezone` column | Not supported |
| Used in | `sched_cron`, Job YAML, `evaluate_schedule.py` | Linux crontab, croniter, APScheduler |

---

## Interval-Based (Every N Minutes)

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 01 | Every 5 min daily | `0 */5 * * * ?` | `*/5 * * * *` | |
| 02 | Every 5 min daily 02:00–23:30 | `0 */5 2-23 * * ?` | `*/5 2-23 * * *` | ¹ |
| 03 | Every 10 min daily | `0 */10 * * * ?` | `*/10 * * * *` | |
| 04 | Every 15 min daily 02:00–23:30 | `0 */15 2-23 * * ?` | `*/15 2-23 * * *` | ¹ |
| 05 | Every 15 min business hours Mon–Fri 09:00–17:00 | `0 */15 9-17 ? * MON-FRI` | `*/15 9-17 * * 1-5` | |
| 06 | Every 30 min daily 02:00–23:30 | `0 */30 2-23 * * ?` | `*/30 2-23 * * *` | ¹ |
| 07 | Every 5 min weekends (Sat & Sun) | `0 */5 * ? * SAT,SUN` | `*/5 * * * 0,6` | |
| 08 | Every 5 min Mon/Wed/Fri | `0 */5 * ? * MON,WED,FRI` | `*/5 * * * 1,3,5` | |
| 09 | Every 5 min Mon/Wed/Fri 02:00–23:30 | `0 */5 2-23 ? * MON,WED,FRI` | `*/5 2-23 * * 1,3,5` | ¹ |

## Hourly

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 10 | Every hour daily | `0 0 * * * ?` | `0 * * * *` | |
| 11 | Every 2 hours daily | `0 0 */2 * * ?` | `0 */2 * * *` | Fires at 0,2,4,…,22 |
| 12 | Every 2 hours daily 06:00–22:00 | `0 0 6-22/2 * * ?` | `0 6-22/2 * * *` | Fires at 6,8,10,…,22 |
| 13 | Every 3 hours daily | `0 0 */3 * * ?` | `0 */3 * * *` | Fires at 0,3,6,9,…,21 |
| 14 | Every 4 hours daily | `0 0 */4 * * ?` | `0 */4 * * *` | Fires at 0,4,8,12,16,20 |
| 15 | Every 6 hours daily | `0 0 */6 * * ?` | `0 */6 * * *` | Fires at 0,6,12,18 |
| 16 | Every 6 hours with offset from 02:00 | `0 0 2/6 * * ?` | `0 2/6 * * *` | Fires at 2,8,14,20 |
| 17 | Every hour daily 02:00–23:00 | `0 0 2-23 * * ?` | `0 2-23 * * *` | |
| 18 | Every hour weekends (Sat & Sun) | `0 0 * ? * SAT,SUN` | `0 * * * 0,6` | |
| 19 | Every hour Mon/Wed/Fri | `0 0 * ? * MON,WED,FRI` | `0 * * * 1,3,5` | |
| 20 | Every hour Mon/Wed/Fri 02:00–23:00 | `0 0 2-23 ? * MON,WED,FRI` | `0 2-23 * * 1,3,5` | |

## Fixed Daily Times

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 21 | Daily at 05:30 | `0 30 5 * * ?` | `30 5 * * *` | |
| 22 | Daily at 05:30 and 17:30 | `0 30 5,17 * * ?` | `30 5,17 * * *` | |
| 23 | Daily at 06:00, 12:00 and 18:00 | `0 0 6,12,18 * * ?` | `0 6,12,18 * * *` | 3 fixed times |
| 24 | Daily at 05:30 weekdays only | `0 30 5 ? * MON-FRI` | `30 5 * * 1-5` | |
| 25 | Daily at 05:30 and 17:30 weekdays | `0 30 5,17 ? * MON-FRI` | `30 5,17 * * 1-5` | |
| 26 | Every 2 hours from 06:00 Mon/Wed | `0 0 6/2 ? * MON,WED` | `0 6/2 * * 1,3` | Fires at 6,8,10,…,22 |

## Weekly

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 27 | Every Monday at 08:00 | `0 0 8 ? * MON` | `0 8 * * 1` | |
| 28 | Every Monday at 08:00 and 20:00 | `0 0 8,20 ? * MON` | `0 8,20 * * 1` | |
| 29 | Every Tue/Thu at 08:00 | `0 0 8 ? * TUE,THU` | `0 8 * * 2,4` | |
| 30 | Mon/Wed/Fri at 08:00 and 20:00 | `0 0 8,20 ? * MON,WED,FRI` | `0 8,20 * * 1,3,5` | |

## Monthly

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 31 | Every month 1st at 08:00 | `0 0 8 1 * ?` | `0 8 1 * *` | |
| 32 | First day of month at 08:00 | `0 0 8 1 * ?` | `0 8 1 * *` | Same as #31 |
| 33 | Last day of month at 08:00 | `0 0 8 L * ?` | — | `L` operator |
| 34 | 2nd Tuesday of every month at 08:00 | `0 0 8 ? * 3#2` | — | `#` operator |
| 35 | Nearest weekday to 15th at 08:00 | `0 0 8 15W * ?` | — | `W` operator |
| 36 | Bi-monthly (1st & 15th) at 08:00 | `0 0 8 1,15 * ?` | `0 8 1,15 * *` | |
| 37 | Bi-monthly 08:00–23:00 hourly | `0 0 8-23 1,15 * ?` | `0 8-23 1,15 * *` | |
| 38 | 15th of month 08:00–23:00 hourly | `0 0 8-23 15 * ?` | `0 8-23 15 * *` | |

## Quarterly

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 39 | First day of quarter at 00:00 | `0 0 0 1 1,4,7,10 ?` | `0 0 1 1,4,7,10 *` | Jan, Apr, Jul, Oct |
| 40 | Last day of quarter at 00:00 | `0 0 0 L 3,6,9,12 ?` | — | `L` operator |
| 41 | First day of quarter 08:00–23:00 hourly | `0 0 8-23 1 1,4,7,10 ?` | `0 8-23 1 1,4,7,10 *` | |
| 42 | Last day of quarter 08:00–23:00 hourly | `0 0 8-23 L 3,6,9,12 ?` | — | `L` operator |

## Yearly

| # | Description | Quartz 6-field | Std 5-field (ref) | Notes |
| --- | --- | --- | --- | --- |
| 43 | First day of year at 00:00 | `0 0 0 1 1 ?` | `0 0 1 1 *` | Jan 1 |
| 44 | Last day of year at 00:00 | `0 0 0 31 12 ?` | `0 0 31 12 *` | Dec 31 |
| 45 | First day of year 08:00–23:00 hourly | `0 0 8-23 1 1 ?` | `0 8-23 1 1 *` | |
| 46 | Last day of year 08:00–23:00 hourly | `0 0 8-23 31 12 ?` | `0 8-23 31 12 *` | |

## Biweekly / Week-Step (Custom Extension)

| # | Description | Quartz 6-field | Notes |
| --- | --- | --- | --- |
| 47 | Every other Monday at 08:00 (even ISO weeks) | `0 0 8 ? * MON/2` | `DOW/N` syntax |
| 48 | Every other Monday at 08:00 (odd ISO weeks) | `0 0 8 ? * MON/2+1` | `DOW/N+offset` syntax |
| 49 | Every 3rd Friday at 08:00 | `0 0 8 ? * FRI/3` | Fires when `iso_week % 3 == 0` |
| 50 | Every other month on 1st at 08:00 (Jan,Mar,May…) | `0 0 8 1 1/2 ?` | Standard step on month field |
| 51 | Every other month on 1st at 08:00 (Feb,Apr,Jun…) | `0 0 8 1 2/2 ?` | Standard step on month field |

## Multi-Cron (Custom Extension)

| # | Description | Quartz 6-field (pipe-separated) | Notes |
| --- | --- | --- | --- |
| 52 | Daily at 07:30, 11:00 and 16:45 (irregular) | `0 30 7 * * ? \| 0 0 11 * * ? \| 0 45 16 * * ?` | Earliest next-fire wins |
| 53 | Daily at 06:00 and 18:00 | `0 0 6 * * ? \| 0 0 18 * * ?` | Equivalent to `0 0 6,18 * * ?` |

---

## Footnotes

**¹ Half-hour cutoff limitation** — Hour range `2-23` fires through `:55` of the last hour (e.g., 23:55 for 5-min intervals). Neither standard cron nor Quartz can express a mid-hour cutoff like 23:30 in a single expression. Options:
- Use two cron entries (one covering hours 2–22, another covering 23:00–23:30)
- Apply application-level gating (check current time before executing)
- Accept the slight overrun (fires until 23:55 instead of 23:30)

---

## Custom Operators (supported by `evaluate_schedule.py`)

The framework's custom Quartz parser supports all standard Quartz operators plus:

| Operator | Field | Meaning | Example |
| --- | --- | --- | --- |
| `L` | day-of-month | Last day of the month | `0 0 8 L * ?` |
| `LW` | day-of-month | Last weekday of the month | `0 0 8 LW * ?` |
| `L-N` | day-of-month | Nth day before last day | `0 0 8 L-3 * ?` → 28th in a 31-day month |
| `NW` | day-of-month | Nearest weekday to day N | `0 0 8 15W * ?` |
| `NL` | day-of-week | Last Nth weekday of the month | `0 0 8 ? * 6L` → last Friday |
| `N#M` | day-of-week | Mth occurrence of weekday N | `0 0 8 ? * 3#2` → 2nd Tuesday |
| `DOW/N` | day-of-week | Every Nth week for that weekday | `0 0 8 ? * MON/2` → every other Monday |
| `DOW/N+offset` | day-of-week | Nth week with offset (parity shift) | `0 0 8 ? * MON/2+1` → alternate Mondays |
| `expr \| expr` | full expression | Multi-cron; earliest next-fire wins | `0 30 7 * * ? \| 0 0 16 * * ?` |

**Week-step logic:** `DOW/N` uses ISO week number parity. `MON/2` fires on Mondays where `iso_week % 2 == 0`. Use `+1` to shift to odd weeks. No anchor column needed — ISO weeks are deterministic.

**Multi-cron:** Pipe-separated expressions are evaluated independently; the earliest `next_fire_time` across all sub-expressions is returned.

**Timezone:** Pass `tz_name` (e.g., `"America/New_York"`) via the existing `sched_timezone` config column. Fire times are computed in local time then converted to UTC. DST transitions are handled automatically by `zoneinfo`.

These operators are evaluated by `_resolve_dow()`, `_next_fire_time()`, and `_next_fire_time_single()` in `evaluate_schedule.py` using only `re`, `calendar`, `datetime`, and `zoneinfo` (stdlib — no external dependencies).

---

## Limitations

The only pattern that remains **not expressible** even with custom extensions:

| Pattern | Why | Workaround |
| --- | --- | --- |
| N times per day at truly arbitrary intervals with different minutes AND hours per occurrence that can't be decomposed | Each sub-expression must be a valid 6-field cron | Use pipe multi-cron (covers most cases) |

Previously listed patterns that are now **fully supported:**

| Pattern | Now supported via |
| --- | --- |
| Every other week (e.g., every other Monday) | `DOW/N` syntax: `0 0 8 ? * MON/2` |
| Every other month | Month step: `0 0 8 1 1/2 ?` (always worked) |
| N times per day at irregular intervals | Multi-cron pipe: `expr1 \| expr2 \| expr3` |
| Timezone-aware DST transitions | `sched_timezone` column + `tz_name` parameter |

---

## Framework Usage

| Context | Format | Example |
| --- | --- | --- |
| `ops_cfg_file_ingestion.sched_cron` | **Quartz 6-field** | `0 0 * * * ?` |
| `ops_cfg_file_ingestion.sched_timezone` | IANA timezone string | `America/New_York` |
| `resources/jobs/*.yml` (Databricks Jobs) | Quartz 6-field | `0 0 * * * ?` |
| `evaluate_schedule.py` (`_next_fire_time`) | Quartz 6-field + extensions | `0 0 8 ? * MON/2` |
| `seeds/config/*.csv` | Quartz 6-field | `0 0 * * * ?` |
| Linux crontab / `croniter` (external) | Standard 5-field | `0 * * * *` |

**Single format everywhere** — the same Quartz expression is used in the CSV seed, synced to the config table, evaluated by `evaluate_schedule.py`, and mirrors the Databricks Job YAML. No translation required.

---

*Last updated: 2026-05-04*
