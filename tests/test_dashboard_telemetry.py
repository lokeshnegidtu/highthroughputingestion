from dashboard.src.app import reconcile_document_lifecycle, evaluate_pipeline_health


def test_reconcile_document_lifecycle_counts_from_db_state():
    counts = {
        "QUEUED": 3,
        "PROCESSING": 2,
        "COMPLETED": 8,
        "FAILED": 1,
        "RETRYING": 1,
        "REJECTED": 1,
    }

    reconciled = reconcile_document_lifecycle(counts)

    assert reconciled["submitted"] == 16
    assert reconciled["processing"] == 6
    assert reconciled["completed"] == 8
    assert reconciled["failed"] == 2


def test_pipeline_health_uses_required_services_only():
    assert evaluate_pipeline_health({
        "api": True,
        "broker": True,
        "database": True,
        "workers": True,
        "minio": False,
    }) == "HEALTHY"

    assert evaluate_pipeline_health({
        "api": True,
        "broker": True,
        "database": False,
        "workers": True,
        "minio": False,
    }) == "DEGRADED"

    assert evaluate_pipeline_health({
        "api": False,
        "broker": False,
        "database": False,
        "workers": False,
        "minio": False,
    }) == "UNHEALTHY"
