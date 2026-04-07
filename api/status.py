"""GET /api/status?job_id=xxx — poll RunPod job status, save result on completion."""

import json
import os

import httpx

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
RUNPOD_ENDPOINT = os.environ["RUNPOD_ENDPOINT"]
GCS_BUCKET = os.environ.get("GCS_BUCKET", "simcon-59f12-media")


async def handler(request):
    if request.method == "OPTIONS":
        return _cors_response(200, "")

    job_id = request.query_params.get("job_id")
    if not job_id:
        return _cors_response(400, json.dumps({"error": "job_id required"}))

    subject = request.query_params.get("subject", "default")
    content_hash = request.query_params.get("content_hash", "")

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{RUNPOD_ENDPOINT}/status/{job_id}",
            headers={"Authorization": f"Bearer {RUNPOD_API_KEY}"},
        )
        data = resp.json()

    status = data.get("status", "UNKNOWN")

    if status == "COMPLETED":
        result = data["output"]
        if content_hash:
            _save_result(content_hash, subject, result)
        return _cors_response(200, json.dumps({
            "status": "COMPLETED",
            "output": result,
            "executionTime": data.get("executionTime"),
        }))

    if status in ("FAILED", "TIMED_OUT", "CANCELLED"):
        return _cors_response(200, json.dumps({
            "status": status,
            "error": data.get("error"),
        }))

    return _cors_response(200, json.dumps({"status": status}))


def _save_result(content_hash: str, subject: str, result: dict):
    try:
        from google.cloud import storage
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_BUCKET)
        bucket.blob(f"results/{subject}/{content_hash}.json").upload_from_string(
            json.dumps(result), content_type="application/json"
        )
    except Exception as e:
        print(f"[status] GCS save failed: {e}")


def _cors_response(status, body):
    from starlette.responses import Response
    return Response(
        content=body,
        status_code=status,
        media_type="application/json",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        },
    )
