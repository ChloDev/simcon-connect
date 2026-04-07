"""POST /api/scan — preprocess video with ffmpeg, submit to RunPod, return job_id."""

import base64
import hashlib
import json
import os
import subprocess
import tempfile

import httpx

RUNPOD_API_KEY = os.environ["RUNPOD_API_KEY"]
RUNPOD_ENDPOINT = os.environ["RUNPOD_ENDPOINT"]
GCS_BUCKET = os.environ.get("GCS_BUCKET", "simcon-59f12-media")

# Path to bundled static ffmpeg/ffprobe binaries
BIN_DIR = os.path.join(os.path.dirname(__file__), "..", "bin")
FFMPEG = os.path.join(BIN_DIR, "ffmpeg")
FFPROBE = os.path.join(BIN_DIR, "ffprobe")


MAX_SIZE_BYTES = 7 * 1024 * 1024  # 7MB
MAX_DURATION = 30  # seconds


def preprocess_video(input_path: str) -> str:
    """Compress video to ≤7MB. Skip if already under limit."""
    file_size = os.path.getsize(input_path)

    # Already under 7MB — no processing needed
    if file_size <= MAX_SIZE_BYTES:
        return input_path

    # Get duration for bitrate calculation
    probe = subprocess.run(
        [FFPROBE, "-v", "quiet", "-print_format", "json",
         "-show_format", "-show_streams", "-select_streams", "v:0", input_path],
        capture_output=True, timeout=10,
    )

    duration = MAX_DURATION
    if probe.returncode == 0:
        info = json.loads(probe.stdout)
        duration = min(float(info.get("format", {}).get("duration", MAX_DURATION)), MAX_DURATION)

    # Target: 7MB total, minus 64kbps audio overhead
    audio_kbps = 64
    target_kbps = int((MAX_SIZE_BYTES * 8 / 1000) / duration) - audio_kbps

    out = tempfile.mktemp(suffix=".mp4")
    result = subprocess.run(
        [FFMPEG, "-y", "-i", input_path,
         "-t", str(MAX_DURATION),
         "-c:v", "libx264", "-preset", "veryfast",
         "-b:v", f"{target_kbps}k", "-maxrate", f"{target_kbps}k",
         "-bufsize", f"{target_kbps * 2}k",
         "-c:a", "aac", "-b:a", f"{audio_kbps}k",
         "-movflags", "+faststart", out],
        capture_output=True, timeout=120,
    )

    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {result.stderr.decode()[:500]}")
    return out


async def handler(request):
    from http import HTTPStatus

    if request.method == "OPTIONS":
        return _cors_response(200, "")

    if request.method != "POST":
        return _cors_response(405, json.dumps({"error": "Method not allowed"}))

    try:
        content_type = request.headers.get("content-type", "")
        if "multipart/form-data" not in content_type:
            return _cors_response(400, json.dumps({"error": "Expected multipart/form-data"}))

        form = await request.form()
        video = form.get("video")
        subject = form.get("subject", "default")
        include_activations = form.get("include_activations", "false").lower() == "true"

        raw = await video.read()
        content_hash = hashlib.sha256(raw).hexdigest()[:16]

        tmp = tempfile.NamedTemporaryFile(suffix=".mp4", delete=False)
        tmp.write(raw)
        tmp.close()

        try:
            processed_path = preprocess_video(tmp.name)
            with open(processed_path, "rb") as f:
                video_b64 = base64.b64encode(f.read()).decode()

            if processed_path != tmp.name:
                os.unlink(processed_path)
        finally:
            os.unlink(tmp.name)

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
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
            submit_data = resp.json()

        if "id" not in submit_data:
            return _cors_response(500, json.dumps({"error": submit_data.get("detail", "Submit failed")}))

        # Save raw video to GCS in background-safe way
        _save_video(raw, content_hash, subject)

        return _cors_response(200, json.dumps({
            "job_id": submit_data["id"],
            "content_hash": content_hash,
            "subject": subject,
        }))

    except Exception as e:
        return _cors_response(500, json.dumps({"error": str(e)}))


def _save_video(video_bytes: bytes, content_hash: str, subject: str):
    try:
        from google.cloud import storage
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_BUCKET)
        bucket.blob(f"videos/{subject}/{content_hash}.mp4").upload_from_string(
            video_bytes, content_type="video/mp4"
        )
    except Exception as e:
        print(f"[scan] GCS upload failed: {e}")


def _cors_response(status, body):
    from starlette.responses import Response
    return Response(
        content=body,
        status_code=status,
        media_type="application/json",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "*",
        },
    )
