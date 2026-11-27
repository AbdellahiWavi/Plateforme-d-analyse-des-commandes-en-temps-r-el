import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient

import analysis

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("insights-service")

ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://localhost:5003").rstrip("/")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
# Les noms de modèles évoluent : vérifier la liste actuelle dans la console Groq.
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
# Option propre aux modèles GPT OSS (low, medium, high) ; laisser vide pour les autres modèles.
GROQ_REASONING_EFFORT = os.getenv("GROQ_REASONING_EFFORT", "")
GROQ_URL = os.getenv("GROQ_API_URL", "https://api.groq.com/openai/v1/chat/completions")
INTERVAL = int(os.getenv("INSIGHTS_INTERVAL_SECONDS", "900"))
WINDOW_HOURS = int(os.getenv("INSIGHTS_WINDOW_HOURS", "6"))
Z_THRESHOLD = float(os.getenv("ANOMALY_Z_THRESHOLD", "3.0"))
REFRESH_COOLDOWN = int(os.getenv("REFRESH_COOLDOWN_SECONDS", "30"))
SCHEDULER_ENABLED = os.getenv("INSIGHTS_SCHEDULER", "true").lower() == "true"
MAX_REPORTS = 200

health_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)  # sondes : échec rapide
db = MongoClient(MONGO_URI)["insights_db"]
reports_col = db["reports"]

generation_lock = threading.Lock()
refresh_lock = threading.Lock()
last_refresh: float | None = None


class LLMUnavailable(Exception):
    pass


# ---------- Accès à analytics-service ----------
def fetch_json(path: str, params: dict | None = None):
    r = httpx.get(f"{ANALYTICS_URL}{path}", params=params, timeout=5.0)
    r.raise_for_status()
    return r.json()


def collect_metrics() -> dict:
    stats = fetch_json("/stats")
    products = fetch_json("/stats/products")
    hourly = fetch_json("/stats/hourly", {"hours": 168})
    customers = fetch_json("/stats/customers")
    return analysis.compute_metrics(
        stats, products, hourly, customers,
        window_hours=WINDOW_HOURS, z_threshold=Z_THRESHOLD,
    )


# ---------- Persistance ----------
def save_report(report: dict) -> None:
    reports_col.insert_one(report.copy())  # copy : Mongo ajoute _id au dict
    old = list(reports_col.find({}, {"_id": 1}).sort("generated_at", -1).skip(MAX_REPORTS))
    if old:
        reports_col.delete_many({"_id": {"$in": [d["_id"] for d in old]}})


def latest_report() -> dict | None:
    return reports_col.find_one({}, {"_id": 0}, sort=[("generated_at", -1)])


def list_reports(limit: int) -> list[dict]:
    cursor = reports_col.find({}, {"_id": 0, "generated_at": 1, "report": 1, "llm": 1})
    return list(cursor.sort("generated_at", -1).limit(limit))


# ---------- Groq ----------
SYSTEM_PROMPT = """Tu es un analyste de données pour une plateforme e-commerce.
Règles strictes :
- Utilise UNIQUEMENT les chiffres du JSON fourni ; n'invente aucune valeur.
- Si une section a le statut "insufficient_data", dis que l'échantillon est trop petit pour conclure.
- N'affirme aucune causalité : propose des hypothèses à vérifier.
- Réponds en français, de façon concise, avec exactement ce format :
Résumé : 2 à 3 phrases.
Points clés : 3 puces maximum.
Anomalies : une phrase.
Recommandations : 2 puces maximum."""


def ask_groq(metrics: dict) -> str:
    """Envoie uniquement des métriques agrégées (jamais de données personnelles)."""
    if not GROQ_API_KEY:
        raise LLMUnavailable("GROQ_API_KEY not set")

    payload = {
        "model": GROQ_MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",
             "content": "Voici les métriques calculées (JSON) :\n" + json.dumps(metrics, ensure_ascii=False)},
        ],
    }
    if GROQ_REASONING_EFFORT:
        payload["reasoning_effort"] = GROQ_REASONING_EFFORT
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}

    for attempt in range(3):
        r = httpx.post(GROQ_URL, json=payload, headers=headers, timeout=30.0)
        if r.status_code == 429 and attempt < 2:
            try:
                wait = float(r.headers.get("retry-after", 2 ** (attempt + 1)))
            except ValueError:
                wait = 2 ** (attempt + 1)
            time.sleep(min(wait, 10))
            continue
        r.raise_for_status()
        text = (r.json()["choices"][0]["message"].get("content") or "").strip()
        if not text:
            raise LLMUnavailable("empty response")
        return text
    raise LLMUnavailable("rate limited")


# ---------- Génération des rapports ----------
def generate_report(force: bool = False) -> dict:
    with generation_lock:
        metrics = collect_metrics()
        previous = latest_report()

        if not force and previous is not None:
            prev_total = ((previous.get("metrics") or {}).get("kpis") or {}).get("total_orders")
            same = prev_total == (metrics.get("kpis") or {}).get("total_orders")
            retry_llm = bool(GROQ_API_KEY) and (previous.get("llm") or {}).get("source") != "groq"
            if same and not retry_llm:
                return previous  # rien de nouveau : on évite un appel inutile à Groq

        llm = {"source": "fallback", "model": None, "error": None}
        try:
            text = ask_groq(metrics)
            llm = {"source": "groq", "model": GROQ_MODEL, "error": None}
        except LLMUnavailable as e:
            llm["error"] = str(e)
            text = analysis.build_fallback_report(metrics)
        except Exception as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            llm["error"] = f"{type(e).__name__}" + (f" (HTTP {status})" if status else "")
            log.warning("Appel Groq impossible : %s", llm["error"])
            text = analysis.build_fallback_report(metrics)

        report = {
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "report": text,
            "llm": llm,
            "metrics": metrics,
        }
        save_report(report)
        return report


def scheduler_loop() -> None:
    time.sleep(20)  # laisse démarrer analytics-service
    while True:
        ok = True
        try:
            generate_report()
        except Exception as e:
            ok = False
            log.warning("Rapport périodique impossible : %s", type(e).__name__)
        time.sleep(INTERVAL if ok else min(INTERVAL, 60))


@asynccontextmanager
async def lifespan(app: FastAPI):
    if SCHEDULER_ENABLED:
        threading.Thread(target=scheduler_loop, daemon=True).start()
    yield


app = FastAPI(
    title="Insights Service",
    description="Analyses avancées des commandes et rapport rédigé par un LLM (Groq)",
    version="1.0.0",
    lifespan=lifespan,
)


def run_or_503(fn, *args):
    try:
        return fn(*args)
    except httpx.HTTPError:
        raise HTTPException(status_code=503, detail="Analytics service unavailable")


# ---------- Health ----------
def mongo_status() -> str:
    try:
        health_client.admin.command("ping")
        return "ok"
    except Exception as e:
        return f"error: {type(e).__name__}"


def analytics_status() -> str:
    try:
        r = httpx.get(f"{ANALYTICS_URL}/health", timeout=2.0)
        return "ok" if r.status_code == 200 else f"error: HTTP {r.status_code}"
    except httpx.HTTPError as e:
        return f"error: {type(e).__name__}"


@app.get("/health")
def health():
    """Liveness : le processus répond."""
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    """Readiness : MongoDB et analytics-service sont joignables (Groq est optionnel : mode dégradé)."""
    checks = {"mongo": mongo_status(), "analytics": analytics_status()}
    ok = all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if ok else 503,
        content={"status": "ready" if ok else "not_ready", "checks": checks},
    )


# ---------- Routes ----------
@app.get("/")
def home():
    return {"service": "Insights Service", "status": "running"}


@app.get("/insights")
def get_insights():
    """Dernier rapport ; généré à la demande s'il n'en existe pas encore."""
    report = latest_report()
    if report is None:
        report = run_or_503(generate_report, True)
    return report


@app.get("/insights/metrics")
def get_metrics():
    """Métriques calculées en direct, sans appel à Groq."""
    return run_or_503(collect_metrics)


@app.post("/insights/refresh")
def refresh():
    """Régénère un rapport (délai minimal entre deux appels, pour protéger le quota Groq)."""
    global last_refresh
    with refresh_lock:
        now = time.monotonic()
        if last_refresh is not None and now - last_refresh < REFRESH_COOLDOWN:
            wait = int(REFRESH_COOLDOWN - (now - last_refresh)) + 1
            raise HTTPException(status_code=429, detail=f"Too many refreshes, retry in {wait}s")
        last_refresh = now
    return run_or_503(generate_report, True)


@app.get("/insights/history")
def history(limit: int = Query(10, ge=1, le=100)):
    return list_reports(limit)
