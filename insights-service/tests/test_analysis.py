from datetime import datetime, timedelta, timezone

import analysis

NOW = datetime(2026, 11, 25, 14, 30, tzinfo=timezone.utc)


def rows(counts, end_hour=13):
    """counts : liste de commandes par heure, la dernière heure terminée étant `end_hour`."""
    start = NOW.replace(hour=end_hour, minute=0) - timedelta(hours=len(counts) - 1)
    return [{"hour": (start + timedelta(hours=i)).strftime(analysis.HOUR_FORMAT), "orders": n}
            for i, n in enumerate(counts)]


def test_series_fills_gaps_and_excludes_current_hour():
    data = [
        {"hour": "2026-11-25T10", "orders": 2},
        {"hour": "2026-11-25T12", "orders": 4},
        {"hour": "2026-11-25T14", "orders": 9},  # heure en cours : exclue de la série
    ]
    series, current = analysis.build_hourly_series(data, NOW)
    assert [p["hour"][-2:] for p in series] == ["10", "11", "12", "13"]
    assert [p["orders"] for p in series] == [2, 0, 4, 0]
    assert current == {"hour": "2026-11-25T14", "orders": 9}


def test_series_empty():
    series, current = analysis.build_hourly_series([], NOW)
    assert series == []
    assert current["orders"] == 0


def test_series_only_current_hour():
    series, current = analysis.build_hourly_series([{"hour": "2026-11-25T14", "orders": 3}], NOW)
    assert series == []
    assert current["orders"] == 3


def test_trend_ok_and_growth():
    series = [{"hour": str(i), "orders": n} for i, n in enumerate([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3, 3])]
    trend = analysis.compute_trend(series, window_hours=6)
    assert trend["status"] == "ok"
    assert trend["previous_orders"] == 9 and trend["current_orders"] == 18
    assert trend["growth_pct"] == 100.0


def test_trend_previous_zero_gives_none_growth():
    series = [{"hour": str(i), "orders": n} for i, n in enumerate([0] * 6 + [2] * 6)]
    assert analysis.compute_trend(series, 6)["growth_pct"] is None


def test_trend_insufficient_data():
    assert analysis.compute_trend([{"hour": "x", "orders": 1}] * 5, 6)["status"] == "insufficient_data"


def test_peak():
    series = [{"hour": "a", "orders": 1}, {"hour": "b", "orders": 7}, {"hour": "c", "orders": 3}]
    assert analysis.find_peak(series)["hour"] == "b"
    assert analysis.find_peak([{"hour": "a", "orders": 0}]) is None
    assert analysis.find_peak([]) is None


def test_anomaly_spike_detected():
    series = rows([4, 5, 4, 5, 4, 5, 4, 5, 40, 4])
    result = analysis.detect_anomalies(series)
    assert result["status"] == "ok"
    assert len(result["anomalies"]) == 1
    a = result["anomalies"][0]
    assert a["type"] == "spike" and a["orders"] == 40


def test_anomaly_drop_detected_when_volume_is_high():
    series = rows([10, 11, 10, 12, 10, 11, 10, 11, 0, 10])
    types = [a["type"] for a in analysis.detect_anomalies(series)["anomalies"]]
    assert types == ["drop"]


def test_no_false_positive_on_tiny_volumes():
    series = rows([0, 1, 0, 0, 1, 0, 2, 0, 1, 0])
    assert analysis.detect_anomalies(series)["anomalies"] == []


def test_anomaly_insufficient_data():
    assert analysis.detect_anomalies(rows([1, 2, 3]))["status"] == "insufficient_data"


def test_forecast_converges_to_constant_level():
    f = analysis.forecast_next_hour(rows([5] * 10))
    assert f["status"] == "ok" and f["next_hour_orders"] == 5.0 and f["confidence"] == "low"


def test_forecast_confidence_medium_with_enough_history():
    assert analysis.forecast_next_hour(rows([5] * 30))["confidence"] == "medium"


def test_forecast_insufficient_data():
    assert analysis.forecast_next_hour(rows([5] * 3))["status"] == "insufficient_data"


def test_segment_rules():
    seg = analysis.segment_customer
    assert seg(5, 2) == "champion"
    assert seg(5, 15) == "loyal"
    assert seg(1, 1) == "new"
    assert seg(2, 10) == "regular"
    assert seg(3, 45) == "at_risk"
    assert seg(1, 45) == "dormant"


def test_rfm_aggregates_without_individual_data():
    customers = [
        {"customer_id": 1, "orders": 4, "revenue": 400.0, "last_order_at": "2026-11-24T10:00:00+00:00"},
        {"customer_id": 2, "orders": 1, "revenue": 100.0, "last_order_at": "2026-11-25T09:00:00+00:00"},
        {"customer_id": 3, "orders": 1, "revenue": 500.0, "last_order_at": "2026-09-01T09:00:00+00:00"},
        {"customer_id": 4, "orders": 2},  # sans date : ignoré
    ]
    rfm = analysis.compute_rfm(customers, NOW)
    assert rfm["status"] == "ok" and rfm["customers"] == 3
    assert set(rfm["segments"]) == {"champion", "new", "dormant"}
    assert rfm["segments"]["dormant"]["revenue_share_pct"] == 50.0
    assert "customer_id" not in str(rfm)


def test_rfm_empty():
    assert analysis.compute_rfm([], NOW)["status"] == "insufficient_data"


def test_compute_metrics_and_fallback_report():
    stats = {"total_orders": 120, "revenue": 10800.0, "average_order_value": 90.0}
    products = [{"product": "Laptop", "orders": 60, "quantity": 70, "revenue": 8000.0}]
    hourly = rows([4, 5] * 6 + [40, 4])
    customers = [{"customer_id": 1, "orders": 4, "revenue": 400.0, "last_order_at": "2026-11-25T10:00:00+00:00"}]

    metrics = analysis.compute_metrics(stats, products, hourly, customers, NOW)
    assert metrics["kpis"] == stats
    assert metrics["anomalies"]["status"] == "ok"
    assert metrics["peak_hour"]["orders"] == 40

    text = analysis.build_fallback_report(metrics)
    assert "120 commande(s)" in text
    assert "Laptop" in text
    assert "spike" in text


def test_fallback_report_with_no_data():
    metrics = analysis.compute_metrics(
        {"total_orders": 0, "revenue": 0, "average_order_value": 0}, [], [], [], NOW)
    text = analysis.build_fallback_report(metrics)
    assert "0 commande(s)" in text
    assert "insuffisant" in text
