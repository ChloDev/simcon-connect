"""GET /api/health — simple health check."""

import json


async def handler(request):
    from starlette.responses import Response
    return Response(
        content=json.dumps({"status": "ok"}),
        status_code=200,
        media_type="application/json",
        headers={"Access-Control-Allow-Origin": "*"},
    )
