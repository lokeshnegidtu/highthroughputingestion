import importlib.util
from pathlib import Path


module_path = Path(__file__).resolve().parents[1] / "dashboard" / "src" / "app.py"
spec = importlib.util.spec_from_file_location("dashboard_app", module_path)
dashboard_app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dashboard_app)


def test_classify_status_codes_for_load_test_reporting():
    assert dashboard_app.classify_load_test_status(202)["bucket"] == "accepted_202"
    assert dashboard_app.classify_load_test_status(429)["bucket"] == "rejected_429"
    assert dashboard_app.classify_load_test_status(400)["bucket"] == "rejected_4xx"
    assert dashboard_app.classify_load_test_status(500)["bucket"] == "failed_5xx"
    assert dashboard_app.classify_load_test_status(0)["bucket"] == "failed_transport"
    assert dashboard_app.classify_load_test_status(None)["bucket"] == "failed_transport"


def test_throughput_and_result_are_only_passed_for_complete_results():
    complete_run = {
        "requested_documents": 500,
        "completed_requests": 500,
        "accepted_202": 450,
        "rejected_429": 30,
        "rejected_4xx": 5,
        "failed_5xx": 0,
        "failed_timeout": 0,
        "failed_transport": 0,
        "p95_latency_ms": 120,
        "actual_duration": 10.0,
    }
    assert dashboard_app.calculate_throughput_docs_per_sec(complete_run) == 45.0
    assert dashboard_app.determine_test_result(complete_run, 500, 10) == "PASS"

    incomplete_run = {
        "requested_documents": 500,
        "completed_requests": 450,
        "accepted_202": 400,
        "rejected_429": 30,
        "rejected_4xx": 5,
        "failed_5xx": 0,
        "failed_timeout": 0,
        "failed_transport": 0,
        "p95_latency_ms": 120,
        "actual_duration": 10.0,
    }
    assert dashboard_app.determine_test_result(incomplete_run, 500, 10) == "FAIL"
