"""
example:
    from viscacha import Client
    from viscacha.server import create_app
    import uvicorn

    client = Client(log_path="jobs.jsonl")
    app = create_app(client)
    uvicorn.run(app, host="0.0.0.0", port=8000)

Endpoints:
    POST /jobs                  enqueue a job
    GET  /jobs                  list jobs (optional ?status=pending|done|failed)
    GET  /jobs/{id}             get one job
    POST /jobs/claim            worker: claim next job of a type
    POST /jobs/{id}/complete    worker: mark done with result
    POST /jobs/{id}/fail        worker: mark failed (retries if retries < max)
"""

from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI, HTTPException, Response
    from pydantic import BaseModel
except ImportError:
    raise ImportError("pip install fastapi uvicorn  to use the HTTP server")

from .client import Client


def create_app(client: Client) -> FastAPI:
    app = FastAPI(title="Viscacha")

    #  job management 

    class EnqueueRequest(BaseModel):
        job_type: str
        max_retries: int = 3
        args: dict = {}

    @app.post("/jobs", status_code=201)
    def enqueue(req: EnqueueRequest):
        handle = client.enqueue(req.job_type, max_retries=req.max_retries, **req.args)
        return {"job_id": handle.id}

    @app.get("/jobs")
    def list_jobs(status: str | None = None):
        return {"jobs": client.jobs(status=status)}

    @app.get("/jobs/{job_id}")
    def get_job(job_id: str):
        job = client.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return job

    #  worker  

    class ClaimRequest(BaseModel):
        job_type: str
        lease_ttl: float = 30.0

    class CompleteRequest(BaseModel):
        lease_id: str
        job: dict
        result: Any = None

    class FailRequest(BaseModel):
        lease_id: str
        job: dict
        error: str

    @app.post("/jobs/claim")
    def claim(req: ClaimRequest):
        result = client._claim(req.job_type, lease_ttl=req.lease_ttl)
        if result is None:
            return Response(status_code=204)
        job, lease_id = result
        return {"job": job, "lease_id": lease_id}

    @app.post("/jobs/{job_id}/complete")
    def complete(job_id: str, req: CompleteRequest):
        try:
            client._complete(req.job, req.lease_id, req.result)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok"}

    @app.post("/jobs/{job_id}/fail")
    def fail(job_id: str, req: FailRequest):
        max_retries = req.job.get("max_retries", 3)
        try:
            client._fail(req.job, req.lease_id, req.error, max_retries)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"status": "ok"}

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job(job_id: str):
        cancelled = client.cancel(job_id)
        if not cancelled:
            raise HTTPException(status_code=404, detail="Job not found or not pending")
        return {"status": "cancelled"}

    return app
