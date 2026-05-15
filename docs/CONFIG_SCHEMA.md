<a id="top"></a>

# Feed Config Schema

> All config columns are prefixed by feature group for self-documentation.
> This document defines the **target** column naming (Phase R5). Current column names are mapped at the bottom.

---

## Table of Contents

 Group | Prefix | Purpose |
-------|--------|---------|
 [Identity](#feed--identity) | `feed_` | Primary feed key |
 [Source](#src--source) | `src_` | Where to read vendor files |
 [Target](#tgt--target) | `tgt_` | Where to write bronze/silver/gold |
 [Batching](#batch--batching) | `batch_` | File grouping limits |
 [Scheduling](#sched--scheduling) | `sched_` | Incremental timing |
 [Schema](#schema--schema-handling) | `schema_` | Read schema policy |
 [Control](#ctl--control-flags) | `ctl_` | Active/hold/sync flags |
 [Notifications](#notify--notifications) | `notify_` | Alert recipients |
 [Directories](#dir--internal-directories) | `_dir_` | Subfolder paths |
 [System](#sys--system-managed) | `_sys_` | Runtime defaults |
 [Governance](#gov--governance-future) | `gov_` | Future classification & access |
 [Column Mapping](#column-name-mapping-old--new) | — | Current → target name mapping |

---

<a id="feed--identity"></a>

### `feed_` — Identity

 Column | Type | Description |
--------|------|-------------|
 `feed_key` | STRING | Primary feed identifier |
 `feed_sub_key` | STRING | Sub-key variant (default: `DEFAULT`) |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="src--source"></a>

### `src_` — Source

 Column | Type | Description |
--------|------|-------------|
 `src_uri` | STRING | Static base path (`s3://`, `abfss://`, `/Volumes/`) |
 `src_subdir` | STRING | Subfolder under managed volume |
 `src_path_template` | STRING | *FUTURE:* template e.g. `year={yyyy}/month={mm}/` |
 `src_path_partition_cols` | STRING | *FUTURE:* comma-separated template variables |
 `src_file_regex` | STRING | Filename pattern regex |
 `src_file_capture_spec` | STRING | Capture group mapping spec |
 `src_file_delimiter` | STRING | Field separator |
 `src_file_has_header` | STRING | `Y` / `N` |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="tgt--target"></a>

### `tgt_` — Target

 Column | Type | Description |
--------|------|-------------|
 `tgt_bronze_table` | STRING | Bronze table name |
 `tgt_silver_table` | STRING | Silver table name |
 `tgt_gold_table` | STRING | Gold table name |
 `tgt_volume` | STRING | UC volume name |
 `tgt_bronze_partition_cols` | STRING | Comma-separated partition columns (✅ **R1 implemented**) |
 `tgt_bronze_partition_type` | STRING | *FUTURE:* `IDENTITY` or `DATE_MONTH` |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="batch--batching"></a>

### `batch_` — Batching

 Column | Type | Description |
--------|------|-------------|
 `batch_max_files` | INT | Max files per batch |
 `batch_max_size_gb` | DOUBLE | Max batch size in GB |

> **Job parameter (not config column):** `batch_max_per_run` (default 20) — caps the total number of batches per ingestion run. Deferred files are picked up on the next dispatch cycle. Override via CLI: `-- --batch_max_per_run 5`.

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="sched--scheduling"></a>

### `sched_` — Scheduling

 Column | Type | Description |
--------|------|-------------|
 `sched_cron` | STRING | Cron expression or alias |
 `sched_timezone` | STRING | Timezone for schedule |
 `sched_selector_type` | STRING | `FILE_MODIFIED_TS` or `FILE_DATE` |
 `sched_lookback_minutes` | INT | Incremental lookback window |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="schema--schema-handling"></a>

### `schema_` — Schema Handling

 Column | Type | Description |
--------|------|-------------|
 `schema_read_policy` | STRING | `FIRST_FILE`, `SEED`, or `AUTO` |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="ctl--control-flags"></a>

### `ctl_` — Control Flags

 Column | Type | Description |
--------|------|-------------|
 `ctl_active` | STRING | `Y`/`N` — feed enabled |
 `ctl_auto_trigger` | STRING | `Y`/`N` — auto-dispatch enabled |
 `ctl_sync_config` | STRING | `Y`/`N` — sync CSV to config table on each dispatch |
 `ctl_maintenance_hold_until` | STRING | ISO timestamp — pause until |
 `ctl_demo_seed_policy` | STRING | `AUTO`, `COPY`, `SKIP` |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="notify--notifications"></a>

### `notify_` — Notifications

 Column | Type | Description |
--------|------|-------------|
 `notify_recipients` | STRING | Comma-separated email or group for alerts |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="dir--internal-directories"></a>

### `_dir_` — Internal Directories

 Column | Type | Description |
--------|------|-------------|
 `_dir_request` | STRING | Request subfolder |
 `_dir_temp` | STRING | Temp subfolder (ZIP extraction) |
 `_dir_discovery` | STRING | Discovery subfolder |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="sys--system-managed"></a>

### `_sys_` — System Managed

 Column | Type | Description |
--------|------|-------------|
 `_sys_default_request_json` | STRING | Default request payload (JSON) |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="gov--governance-future"></a>

### `gov_` — Governance (FUTURE)

 Column | Type | Description |
--------|------|-------------|
 `gov_data_owner` | STRING | Business owner email/group |
 `gov_data_steward` | STRING | Technical steward email/group |
 `gov_classification` | STRING | `PHI`, `PII`, `INTERNAL`, `PUBLIC` |
 `gov_retention_days` | INT | Data retention in days |
 `gov_access_group` | STRING | IAM/AD group for access |

<p align="right"><a href="#top">↑ top</a></p>

---

<a id="column-name-mapping-old--new"></a>

## Column Name Mapping (old → new)

> This mapping is for **Phase R5** (config column rename). Current code uses the **old names**.

 Old name (current) | New name (target) | Group |
---------------------|-------------------|-------|
 `file_config_key` | `feed_key` | Identity |
 `file_config_sub_key` | `feed_sub_key` | Identity |
 `vendor_source_uri` | `src_uri` | Source |
 `source_subdir` | `src_subdir` | Source |
 `filename_regex` | `src_file_regex` | Source |
 `filename_capture_spec` | `src_file_capture_spec` | Source |
 `delimiter` | `src_file_delimiter` | Source |
 `has_header` | `src_file_has_header` | Source |
 `bronze_table_name` | `tgt_bronze_table` | Target |
 `silver_table_name` | `tgt_silver_table` | Target |
 `gold_table_name` | `tgt_gold_table` | Target |
 `volume_name` | `tgt_volume` | Target |
 `tgt_bronze_partition_cols` | `tgt_bronze_partition_cols` | Target |
 `max_file_count_per_batch` | `batch_max_files` | Batching |
 `max_size_gb_per_batch` | `batch_max_size_gb` | Batching |
 `schedule_cron` | `sched_cron` | Scheduling |
 `schedule_timezone` | `sched_timezone` | Scheduling |
 `incremental_selector_type` | `sched_selector_type` | Scheduling |
 `incremental_lookback_minutes` | `sched_lookback_minutes` | Scheduling |
 `read_schema_policy` | `schema_read_policy` | Schema |
 `flg_active` | `ctl_active` | Control |
 `flg_auto_trigger` | `ctl_auto_trigger` | Control |
 `flg_update_config_table` | `ctl_sync_config` | Control |
 `maintenance_hold_until` | `ctl_maintenance_hold_until` | Control |
 `demo_seed_policy` | `ctl_demo_seed_policy` | Control |
 `notification_recipients` | `notify_recipients` | Notifications |
 `dir_request` | `_dir_request` | Directories |
 `dir_temp` | `_dir_temp` | Directories |
 `dir_discovery` | `_dir_discovery` | Directories |
 `sys_default_request_json` | `_sys_default_request_json` | System |

<p align="right"><a href="#top">↑ back to top</a></p>
