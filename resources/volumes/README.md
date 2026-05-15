Unity Catalog **volumes** for each feed are created by the **dispatcher** path (`ensure_feed_environment` → `CREATE VOLUME IF NOT EXISTS` in SQL), not by anything under this folder.

This `resources/volumes` directory is only a **placeholder** in the repo layout for optional future bundle resources or notes; it is not referenced by `databricks.yml`.
