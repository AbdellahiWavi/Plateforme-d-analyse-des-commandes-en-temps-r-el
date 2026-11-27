"""Calculs d'analyse : fonctions pures, sans réseau ni base de données (donc testables)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev

HOUR_FORMAT = "%Y-%m-%dT%H"


def _hour_floor(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def parse_hour(label: str) -> datetime:
    return datetime.strptime(label, HOUR_FORMAT).replace(tzinfo=timezone.utc)


def build_hourly_series(rows: list[dict], now: datetime | None = None):
    """Construit la série horaire continue des heures **terminées** (heures sans commande = 0).

    Retourne (série, heure_en_cours). L'heure en cours est exclue de la série car
    elle est incomplète : elle fausserait les anomalies et la prévision.
    """
    now_hour = _hour_floor(now or datetime.now(timezone.utc))
    counts = {parse_hour(r["hour"]): r.get("orders", 0) for r in rows}
    current = {"hour": now_hour.strftime(HOUR_FORMAT), "orders": counts.get(now_hour, 0)}

    past = [h for h in counts if h < now_hour]
    if not past:
        return [], current

    series = []
    t = min(past)
    last = now_hour - timedelta(hours=1)
    while t <= last:
        series.append({"hour": t.strftime(HOUR_FORMAT), "orders": counts.get(t, 0)})
        t += timedelta(hours=1)
    return series, current


def compute_trend(series: list[dict], window_hours: int = 6) -> dict:
    """Compare la dernière fenêtre à la précédente (même durée)."""
    n = len(series)
    required = 2 * window_hours
    if n < required:
        return {"status": "insufficient_data", "window_hours": window_hours,
                "history_hours": n, "required_hours": required}
    current = sum(p["orders"] for p in series[-window_hours:])
    previous = sum(p["orders"] for p in series[-required:-window_hours])
    growth = round((current - previous) / previous * 100, 1) if previous else None
    return {"status": "ok", "window_hours": window_hours, "current_orders": current,
            "previous_orders": previous, "growth_pct": growth}


def find_peak(series: list[dict]) -> dict | None:
    if not series:
        return None
    best = max(series, key=lambda p: p["orders"])
    return best if best["orders"] > 0 else None


def detect_anomalies(series: list[dict], z_threshold: float = 3.0,
                     min_points: int = 6, min_std: float = 1.0) -> dict:
    """Détection par z-score : chaque heure est comparée à toutes les autres.

    `min_std` évite de signaler des écarts insignifiants quand les volumes sont très faibles.
    Une chute n'est signalée que si le volume moyen est d'au moins 3 commandes par heure.
    """
    n = len(series)
    if n < min_points:
        return {"status": "insufficient_data", "points": n, "required": min_points, "anomalies": []}

    anomalies = []
    for i, point in enumerate(series):
        others = [q["orders"] for j, q in enumerate(series) if j != i]
        mu = mean(others)
        sd = max(pstdev(others), min_std)
        z = (point["orders"] - mu) / sd
        if z >= z_threshold or (z <= -z_threshold and mu >= 3):
            anomalies.append({
                "hour": point["hour"],
                "orders": point["orders"],
                "expected": round(mu, 2),
                "z_score": round(z, 2),
                "type": "spike" if z > 0 else "drop",
            })
    return {"status": "ok", "points": n, "z_threshold": z_threshold, "anomalies": anomalies}


def forecast_next_hour(series: list[dict], alpha: float = 0.3, min_points: int = 6) -> dict:
    """Prévision simple par lissage exponentiel (volontairement transparente)."""
    n = len(series)
    if n < min_points:
        return {"status": "insufficient_data", "points": n, "required": min_points}
    level = series[0]["orders"]
    for p in series[1:]:
        level = alpha * p["orders"] + (1 - alpha) * level
    return {
        "status": "ok",
        "method": "exponential_smoothing",
        "alpha": alpha,
        "next_hour_orders": round(level, 2),
        "confidence": "low" if n < 24 else "medium",
    }


def segment_customer(orders: int, recency_days: float) -> str:
    """Segmentation RFM simplifiée, par règles (peu de clients : les quantiles n'auraient pas de sens)."""
    if recency_days > 30:
        return "at_risk" if orders >= 2 else "dormant"
    if orders >= 3:
        return "champion" if recency_days <= 7 else "loyal"
    if orders == 1:
        return "new"
    return "regular"


def compute_rfm(customers: list[dict], now: datetime | None = None) -> dict:
    """Agrège les clients par segment. Aucune donnée individuelle n'est renvoyée."""
    now = now or datetime.now(timezone.utc)
    segments: dict[str, dict] = {}
    total_revenue = 0.0
    counted = 0

    for c in customers:
        last_raw = c.get("last_order_at")
        if not last_raw:
            continue
        last = datetime.fromisoformat(last_raw)
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        recency_days = max((now - last).total_seconds() / 86400, 0)
        name = segment_customer(int(c.get("orders", 0)), recency_days)
        seg = segments.setdefault(name, {"customers": 0, "revenue": 0.0})
        revenue = float(c.get("revenue", 0))
        seg["customers"] += 1
        seg["revenue"] += revenue
        total_revenue += revenue
        counted += 1

    if counted == 0:
        return {"status": "insufficient_data", "customers": 0, "segments": {}}

    for seg in segments.values():
        seg["revenue_share_pct"] = round(seg["revenue"] / total_revenue * 100, 1) if total_revenue else 0.0
        seg["revenue"] = round(seg["revenue"], 2)
    return {"status": "ok", "customers": counted, "segments": segments}


def compute_metrics(stats: dict, products: list[dict], hourly_rows: list[dict],
                    customers: list[dict], now: datetime | None = None,
                    window_hours: int = 6, z_threshold: float = 3.0) -> dict:
    now = now or datetime.now(timezone.utc)
    series, current = build_hourly_series(hourly_rows, now)
    return {
        "kpis": stats,
        "top_products": products[:5],
        "history_hours": len(series),
        "current_hour": current,
        "trend": compute_trend(series, window_hours),
        "peak_hour": find_peak(series),
        "anomalies": detect_anomalies(series, z_threshold),
        "forecast": forecast_next_hour(series),
        "customer_segments": compute_rfm(customers, now),
    }


def _fmt_hour(label: str) -> str:
    return f"{label[:10]} {label[11:13]}h UTC"


def build_fallback_report(metrics: dict) -> str:
    """Rapport déterministe, utilisé quand le LLM est indisponible ou non configuré."""
    kpis = metrics.get("kpis") or {}
    lines = [
        f"Résumé : {kpis.get('total_orders', 0)} commande(s) au total, "
        f"chiffre d'affaires {kpis.get('revenue', 0):.2f}, "
        f"panier moyen {kpis.get('average_order_value', 0):.2f}."
    ]

    products = metrics.get("top_products") or []
    if products:
        top = products[0]
        lines.append(f"Produit le plus vendu : {top['product']} ({top['orders']} commande(s)).")

    trend = metrics.get("trend") or {}
    if trend.get("status") == "ok":
        if trend.get("growth_pct") is None:
            lines.append(f"Tendance : {trend['current_orders']} commande(s) sur les "
                         f"{trend['window_hours']} dernières heures, aucune sur la période précédente.")
        else:
            lines.append(f"Tendance : {trend['current_orders']} commande(s) sur les "
                         f"{trend['window_hours']} dernières heures contre {trend['previous_orders']} "
                         f"auparavant ({trend['growth_pct']:+.1f} %).")
    else:
        lines.append(f"Tendance : historique insuffisant (au moins {trend.get('required_hours', '?')} "
                     f"heures nécessaires, {trend.get('history_hours', 0)} disponibles).")

    peak = metrics.get("peak_hour")
    if peak:
        lines.append(f"Heure de pointe : {_fmt_hour(peak['hour'])} avec {peak['orders']} commande(s).")

    anomalies = metrics.get("anomalies") or {}
    if anomalies.get("status") != "ok":
        lines.append("Anomalies : historique insuffisant pour en détecter.")
    elif anomalies["anomalies"]:
        details = "; ".join(
            f"{_fmt_hour(a['hour'])} ({a['type']}, {a['orders']} commande(s) pour {a['expected']} attendues)"
            for a in anomalies["anomalies"][:5])
        lines.append(f"Anomalies : {len(anomalies['anomalies'])} détectée(s) : {details}.")
    else:
        lines.append("Anomalies : aucune détectée.")

    forecast = metrics.get("forecast") or {}
    if forecast.get("status") == "ok":
        lines.append(f"Prévision : environ {forecast['next_hour_orders']} commande(s) la prochaine heure "
                     f"(confiance {forecast['confidence']}).")

    segments = metrics.get("customer_segments") or {}
    if segments.get("status") == "ok":
        parts = ", ".join(f"{name} : {seg['customers']}" for name, seg in sorted(segments["segments"].items()))
        lines.append(f"Clients ({segments['customers']}) par segment : {parts}.")
    return "\n".join(lines)
