# Notification Channels Plan

> Planning document for expanding notification delivery from email-only to
> email + Slack + Teams, controlled per feed_key. **Not yet implemented.**
>
> Status: **PARKED** — blocked on Databricks serverless outbound DNS.
> Resume when Network IT resolves the DNS restriction or user is ready
> to implement with a classic cluster workaround.
>
> Last updated: 2026-04-22

---

## Current State

- **Email: DONE** — Databricks job-level `email_notifications` (on_success,
  on_failure) + `ops_notifications` table with `resolved_recipients` routing
  (principle #18: dev→override, prod→per-feed DLs).
- **Slack: PARKED** — self-service available, webhook not yet created.
  Blocked on same DNS restriction as Teams.
- **Teams: PARKED** — legacy Incoming Webhook connector created on
  RI Technology Solutions > `file-ingestion-alerts` channel. Tested
  successfully from local machine. Blocked on DNS from Databricks compute.

The `ops_notifications` table is write-only today — records are logged but
not delivered to any external channel. Delivery is the gap.

---

## Network Blocker: Databricks Serverless → ALL External Endpoints

Databricks serverless compute **cannot resolve any external DNS**. Tested
and confirmed — all external hosts fail:

| Host | Result |
| --- | --- |
| `hooks.slack.com` | DNS FAILED |
| `api.slack.com` | DNS FAILED |
| `slack.com` | DNS FAILED |
| `aetnao365.webhook.office.com` | DNS FAILED |
| `google.com` | DNS FAILED |
| `pypi.org` | DNS FAILED |
| `github.com` | DNS FAILED |
| `login.microsoftonline.com` | DNS FAILED |

**This is a workspace-level network restriction**, not a domain-specific
firewall rule. No external HTTP POST from serverless compute will work.

**Impact by delivery option:**

| Option | Affected? | Workaround |
| --- | --- | --- |
| **A: Inline** | Yes — runs on same serverless compute as pipeline | Requires IT network rule for serverless outbound HTTPS |
| **B: Async job** | Depends — can run on a classic cluster with outbound access | Use a classic cluster with proper network config |
| **C: SQL Alert** | No — Databricks manages delivery from its own infrastructure | N/A |

**To resume:** Request Network IT to allow outbound HTTPS from Databricks
compute, or test whether a classic cluster has different network access.

---

## Teams Setup (Verified)

**Method used:** Legacy Incoming Webhook Connector (Check 2 below).

1. Created channel `file-ingestion-alerts` under RI Technology Solutions
2. Channel `...` → Manage channel → Connectors → Incoming Webhook → Configure
3. Named: `file-ingestion-alerts` → Create → copied webhook URL
4. Tested from local machine: `curl -H "Content-Type: application/json" -d '{"text":"Hello from ingestion framework"}' <URL>` → message appeared in channel

> Microsoft deprecation warning: Office 365 Connectors within Teams will be
> retired soon. The **Workflows app** (Power Automate) is the long-term
> replacement. Legacy connector works today and is sufficient for dev/testing.

### Teams Self-Service Reference

| Check | Method | Available? |
| --- | --- | --- |
| Check 1 — Power Automate Workflows | Channel `+` tab → search "Workflows" | Not checked |
| Check 2 — Legacy Incoming Webhook | Channel `...` → Manage channel → Connectors | **Yes — used** |
| Check 3 — IT-managed | Neither available | N/A |

---

## Slack Setup (Self-Service)

1. Go to [api.slack.com/apps](https://api.slack.com/apps) → **Create New App** → "From scratch"
2. Name: `file-ingestion-alerts` (or similar), pick your dev workspace
3. **Incoming Webhooks** → toggle ON → **Add New Webhook to Workspace**
4. Pick a channel (e.g., `#ingestion-alerts`) → Authorize
5. Copy the webhook URL

Test:
```bash
curl -X POST -H 'Content-type: application/json' \
  --data '{"text":"Hello from ingestion framework"}' \
  https://hooks.slack.com/services/YOUR/WEBHOOK/URL
```

---

## Config Impact: Per-Feed Channel Control

### Proposed: New columns approach

Add two columns to the config CSV alongside existing `notify_recipients`:

| Column | Type | Example | Purpose |
| --- | --- | --- | --- |
| `notify_recipients` (existing) | STRING | `dl-team@aetna.com` | Email DL or address |
| `notify_slack_webhook` (new) | STRING | `https://hooks.slack.com/services/T.../B.../xxx` | Slack Incoming Webhook URL |
| `notify_teams_webhook` (new) | STRING | `https://outlook.office.com/webhook/...` | Teams Webhook URL |

**Per-feed control:** Each feed_key row has its own webhook URLs. Leave blank
to disable a channel for that feed. The framework sends to all non-empty
channels.

**Environment routing (principle #18):** The existing override pattern applies:
- Dev/test: environment-level override webhook URLs (one Slack channel for all feeds)
- Prod: per-feed webhook URLs from config

### Alternative: JSON column approach

Single column `notify_channels_json`:
```json
{"email":"dl-team@aetna.com","slack":"https://hooks.slack.com/...","teams":"https://..."}
```

Trade-off: more flexible but harder to audit in CSV.

### Recommendation

New columns approach — explicit, visible in CSV, follows the existing pattern
of `notify_recipients`. Decision deferred to user.

---

## Delivery Options

| Option | Description | Pipeline impact | Effort |
| --- | --- | --- | --- |
| **A: Inline** | `deliver()` call after each `write_rows` to notifications table | 100-300ms latency per notification | Small — `notify.py` only |
| **B: Async job** | Separate job polls `ops_notifications` for unsent records, delivers, marks sent | Zero pipeline impact | Medium — new job + delivery status tracking |
| **C: SQL Alert** | Databricks SQL Alert on `ops_notifications`, routes via Databricks Notification Destinations | Zero code changes | Small — but see limitations below |

### Option C: SQL Alert — Detailed Findings

SQL Alerts deliver through **Databricks Notification Destinations** (workspace
admin feature), not raw webhook URLs. Setup requirements per channel:

| Destination | Required credentials | Self-service? |
| --- | --- | --- |
| **Slack** | Webhook URL + Bot User OAuth Token + Channel ID | Yes — from Slack app OAuth & Permissions |
| **Teams** | Webhook URL + App ID (Copilot Studio bot) + Auth Secret (Entra ID) + Channel URL + Tenant ID | Likely IT — requires Copilot Studio bot + Entra ID app registration |

Setup path: Settings → Workspace admin → Notifications → Manage → Add
destination → Select Slack or Teams → Enter credentials.

### Option comparison for per-feed control

| Requirement | A (Inline) | B (Async) | C (SQL Alert) |
| --- | --- | --- | --- |
| Per-feed webhook routing | Yes | Yes | No — alert-level only |
| Message formatting control | Full | Full | Limited |
| Pipeline latency | Yes (small) | None | None |
| Retry on delivery failure | Manual | Built-in | Built-in |
| New job to maintain | No | Yes | No |
| `ops_notifications` schema change | No | Yes (`delivery_status`) | No |
| Serverless DNS blocker | **Blocked** | **Avoidable** (classic cluster) | **Not affected** |
| Setup complexity | Low | Medium | High (Teams needs Copilot Studio + Entra ID) |

### Assessment

**Option C is impractical for Teams** — requires Copilot Studio bot and Entra
ID app registration (likely IT involvement). Also lacks per-feed routing.

**Option B is the strongest fit** — per-feed control, full formatting, retry
built-in, and sidesteps the serverless DNS blocker by running on a classic
cluster with outbound access.

**Option A is simplest for dev** if the DNS blocker is resolved.

---

## Blast Radius

| File | Change needed | Risk |
| --- | --- | --- |
| `seeds/config/*.csv` | Add `notify_slack_webhook`, `notify_teams_webhook` columns | Low — additive |
| `framework/constants.py` — `CONFIG_COLUMNS` | Add column definitions | Low — additive |
| `scan_config.py` — `_defaults_for_csv_row()` | Add defaults (empty string) | Low — additive |
| `notify.py` — `resolve_recipients()` | Extend to resolve per channel type | Medium — logic change |
| `notify.py` — new `deliver()` function | HTTP POST to Slack/Teams webhook URLs | Medium — new code |
| `run_request_intake.py` | **No change** | None |
| `write_to_bronze.py` | **No change** | None |
| `dispatch_feeds.py` | **No change** | None |
| `ops_notifications` table | Option B only: add `channel`, `delivery_status` | Low if Option A; Medium if Option B |

**Key point:** The 6 existing call sites that produce notifications do NOT
change under any option. Delivery is abstracted in `notify.py`.

---

## Webhook URL Security

Webhook URLs are secrets — they allow anyone with the URL to post messages.

| Approach | Description |
| --- | --- |
| **Config CSV (simplest for dev)** | URL in plain text in CSV. Acceptable for dev/test. |
| **Databricks Secrets (prod)** | Store URLs in a secret scope. Config stores the secret key name, code resolves at runtime via `dbutils.secrets.get()`. |

For dev/testing, plain text in CSV is fine. Prod should use Databricks Secrets.
This is an implementation-time decision, not a blocker for planning.

---

## Open Decisions (parked)

- [x] Teams: which webhook method is available → **Legacy Incoming Webhook (confirmed)**
- [x] DNS: all external DNS blocked from serverless → **confirmed, workspace-level restriction**
- [ ] **PREREQUISITE:** Network IT request for serverless outbound HTTPS, or classic cluster test
- [ ] Slack: create webhook and verify from local machine
- [ ] Config approach: new columns vs JSON column
- [ ] Delivery option: A (inline), B (async job), or C (SQL Alert)
- [ ] Webhook URL storage: plain text (dev) vs Databricks Secrets (prod)
- [ ] Message format: plain text vs rich formatting (Slack blocks / Teams Adaptive Cards)
- [ ] Environment override: single dev webhook for all feeds, or per-feed even in dev

---

## Next Steps (when resuming)

1. ~~Check Teams self-service availability~~ → **Done** (legacy connector)
2. ~~Test DNS from Databricks compute~~ → **Done** (all external DNS blocked)
3. Resolve network blocker (IT request or classic cluster test)
4. Create a Slack webhook for dev testing
5. Test Slack from local machine
6. Decide on delivery option (A/B/C) — Option B recommended
7. Decide on config approach (new columns vs JSON)
8. Allocate a branch name for implementation
