from src.api.app import app


def test_openapi_includes_control_results_and_metrics() -> None:
    schema = app.openapi()

    assert "/api/control/results" in schema["paths"]
    assert "/api/metrics" in schema["paths"]
    assert "/api/metrics/prometheus" in schema["paths"]
    assert "/api/orders/cancel-bot-open" in schema["paths"]
    assert "/api/control/preview/cancel-bot-open" in schema["paths"]
    assert "/api/control/preview/cancel-all" in schema["paths"]


def test_openapi_marks_operator_routes_with_bearer_security() -> None:
    schema = app.openapi()

    assert "HTTPBearer" in schema["components"]["securitySchemes"]
    assert schema["paths"]["/api/status"]["get"]["security"] == [{"HTTPBearer": []}]
    assert schema["paths"]["/api/orders/cancel-all"]["post"]["security"] == [
        {"HTTPBearer": []}
    ]
