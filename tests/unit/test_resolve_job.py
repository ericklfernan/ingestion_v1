from pipelines.file_ingestion.orchestrate.dispatch_feeds import _resolve_ingestion_job_id


def test_resolve_ingestion_job_id_prefers_numeric():
    jid, err = _resolve_ingestion_job_id({"ingestion_job_id": 12345, "ingestion_job_name": "other_name"})
    assert jid == 12345
    assert err is None


def test_resolve_ingestion_job_id_invalid_numeric():
    jid, err = _resolve_ingestion_job_id({"ingestion_job_id": "not-int", "ingestion_job_name": "x"})
    assert jid is None
    assert err and "integer" in err


def test_resolve_ingestion_job_id_requires_name_when_id_missing():
    jid, err = _resolve_ingestion_job_id({"ingestion_job_id": None, "ingestion_job_name": ""})
    assert jid is None
    assert err and "ingestion_job_name" in err
