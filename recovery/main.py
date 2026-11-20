import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict

from recovery.engine import Engine, report_csv, validate_partition
from recovery.errors import RecoveryError
from recovery.source import FIXTURES
from recovery.store import Store

STATIC = Path(__file__).parent / "static"


class JobRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    action: Literal["broken", "diagnose", "recover"]
    scenario: Literal["schema", "cast", "join"] = "schema"
    partition: str = "2026-11-18"
    strict: bool = False


@asynccontextmanager
async def lifespan(app):
    app.state.store = Store(Path(os.environ.get("RECOVERY_DB_PATH", "data/recovery.sqlite3")))
    app.state.engine = Engine(app.state.store)
    app.state.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="recovery")
    app.state.future = None
    yield
    app.state.pool.shutdown(wait=True)
    app.state.engine.close()


app = FastAPI(title="ETL Pipeline Recovery", lifespan=lifespan, docs_url=None, redoc_url=None)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.middleware("http")
async def headers(request: Request, call_next):
    length = request.headers.get("content-length", "0")
    if not length.isdigit() or int(length) > 16384:
        return JSONResponse(
            {"error": {"code": "request_size", "message": "Job configuration is too large."}},
            status_code=413,
        )
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
    )
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.exception_handler(RecoveryError)
async def recovery_error(request, exc):
    status = 409 if exc.code == "job_active" else 404 if exc.code == "run_not_found" else 422
    return JSONResponse({"error": {"code": exc.code, "message": exc.message}}, status_code=status)


@app.exception_handler(RequestValidationError)
async def bad_request(request, exc):
    return JSONResponse(
        {
            "error": {
                "code": "invalid_request",
                "message": "Choose a supported lab action and valid configuration.",
            }
        },
        status_code=422,
    )


@app.get("/")
def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/health")
def health(request: Request):
    return {
        "status": "ok",
        "spark_initialized": request.app.state.engine._spark is not None,
        "execution": "Real local PySpark; no simulated execution fallback.",
        "data_scope": "Authored synthetic incident.",
    }


@app.get("/api/summary")
def summary(request: Request):
    return request.app.state.store.summary()


@app.post("/api/jobs", status_code=202)
def start_job(body: JobRequest, request: Request):
    validate_partition(body.partition)
    store = request.app.state.store
    run_id = store.start(
        body.action, body.partition, body.scenario if body.action == "broken" else None
    )
    try:
        request.app.state.future = request.app.state.pool.submit(
            request.app.state.engine.execute, run_id, FIXTURES, body.strict
        )
    except RuntimeError as exc:
        store.finish(
            {
                "run_id": run_id,
                "state": "failed",
                "error": {
                    "code": "worker_stopped",
                    "message": "The local worker stopped before dispatch.",
                },
            }
        )
        raise RecoveryError("worker_stopped", "The local worker is stopping.") from exc
    return {"run_id": run_id, "state": "running"}


@app.get("/api/runs/{run_id}")
def run_detail(run_id: str, request: Request):
    return request.app.state.store.run(run_id, detail=True)


@app.get("/api/runs/{run_id}/download/{kind}")
def download(run_id: str, kind: Literal["report", "logs", "csv"], request: Request):
    run = request.app.state.store.run(run_id, detail=True)
    if run["report"] is None:
        raise RecoveryError("report_pending", "Wait until this execution has a terminal report.")
    if kind == "csv":
        content = report_csv(run["report"])
        media = "text/csv"
        name = "outcomes.csv"
    else:
        content = json.dumps(
            run["events"] if kind == "logs" else run["report"], indent=2, ensure_ascii=False
        )
        media = "application/json"
        name = "execution-logs.json" if kind == "logs" else "recovery-report.json"
    return Response(
        content, media_type=media, headers={"Content-Disposition": f'attachment; filename="{name}"'}
    )
