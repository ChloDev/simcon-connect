"""FastAPI preprocessing server — Cloud Run / Firebase deployment.

Downscales video with ffmpeg before forwarding to RunPod.
Reads config from environment variables; PORT is set automatically by Cloud Run.
"""

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import tempfile

import httpx
from fastapi import FastAPI, UploadFile, File, Query
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
RUNPOD_ENDPOINT = os.environ["RUNPOD_ENDPOINT"]  # e.g. https://api.runpod.ai/v2/<id>
GCS_BUCKET = os.environ.get("GCS_BUCKET", "simcon-59f12-media")

gcs = storage.Client()


def preprocess_video(input_path: str) -> str:
    """Downscale video to 480p 30fps. Returns path to processed file."""
    probe = subprocess.run(
        ["ffprobe", "-v", "quiet", "-print_format", "json",
         "-show_streams", "-select_streams", "v:0", input_path],
        capture_output=True, timeout=10,
    )

    w = h = fps = 0
    if probe.returncode == 0:
        s = json.loads(probe.stdout).get("streams", [{}])[0]
        w, h = int(s.get("width", 0)), int(s.get("height", 0))
        r = s.get("r_frame_rate", "30/1").split("/")
        fps = round(int(r[0]) / max(int(r[1]), 1))

    if w <= 640 and fps <= 30:
        return input_path

    out = tempfile.mktemp(suffix=".mp4")
    result = subprocess.run(
        ["ffmpeg", "-y", "-i", input_path,
         "-vf", "scale=-2:480,fps=30",
         "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
         "-c:a", "aac", "-b:a", "64k",
         "-movflags", "+faststart", out],
        capture_output=True, timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr.decode()[:500]}")
    return out


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


@app.post("/scan")
async def scan(
    video: UploadFile = File(...),
    subject: str = Query("default"),
    include_activations: bool = Query(False),
):
    raw = await video.read()
    content_hash = hash_bytes(raw)

    tmp = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
    tmp.write(raw)
    tmp.close()

    try:
        processed_path = preprocess_video(tmp.name)
        with open(processed_path, "rb") as f:
            video_b64 = base64.b64encode(f.read()).decode()

        size_mb = len(video_b64) * 3 / 4 / 1_048_576
        print(f"[Server] Preprocessed: {len(raw)/1_048_576:.1f}MB -> {size_mb:.1f}MB")

        if processed_path != tmp.name:
            os.unlink(processed_path)

        async with httpx.AsyncClient(timeout=300) as client:
            submit = await client.post(
                f"{RUNPOD_ENDPOINT}/run",
                headers={
                    "Authorization": f"Bearer {RUNPOD_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "input": {
                        "video_b64": video_b64,
                        "subject": subject,
                        "include_activations": include_activations,
                        "content_hash": content_hash,
                    }
                },
            )
            submit_data = submit.json()
            if "id" not in submit_data:
                return {"error": submit_data.get("detail", "Submit failed")}

            job_id = submit_data["id"]
            print(f"[Server] Job submitted: {job_id}")

            for _ in range(120):
                await asyncio.sleep(3)
                status = await client.get(
                    f"{RUNPOD_ENDPOINT}/status/{job_id}",
                    headers={"Authorization": f"Bearer {RUNPOD_API_KEY}"},
                )
                data = status.json()

                if data["status"] == "COMPLETED":
                    print(f"[Server] Job completed in {data.get('executionTime', '?')}ms")
                    
                    result = data["output"]
                    _save_to_gcs(raw, content_hash, subject, result)
                    return result
                if data["status"] in ("FAILED", "TIMED_OUT", "CANCELLED"):
                    return {"error": f"Job {data['status']}", "detail": data.get("error")}

            return {"error": "Timed out waiting for result"}
    finally:
        os.unlink(tmp.name)


def _save_to_gcs(video_bytes: bytes, content_hash: str, subject: str, result: dict):
    bucket = gcs.bucket(GCS_BUCKET)
    bucket.blob(f"videos/{subject}/{content_hash}.mp4").upload_from_string(
        video_bytes, content_type="video/mp4"
    )
    bucket.blob(f"results/{subject}/{content_hash}.json").upload_from_string(
        json.dumps(result), content_type="application/json"
    )
    print(f"[Server] Saved video + result to GCS: {content_hash}")


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
