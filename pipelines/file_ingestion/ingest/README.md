# Ingest
Read source files (CSV, ZIP), detect schema drift, write to bronze tables.
Error-resilient: per-file try/except with FAILED status on error.
