"""Локальный приемник webhook, включается профилем compose «demo»."""

import logging

from fastapi import FastAPI, Request, Response

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
logger = logging.getLogger("uvicorn.error")


@app.post("/webhook")
async def webhook(request: Request):
    body = await request.json()
    logger.info("Webhook: payment_id=%s status=%s", body["payment_id"], body["status"])
    return {"received": True}


@app.post("/fail")
async def failing_webhook():
    return Response(status_code=503)
