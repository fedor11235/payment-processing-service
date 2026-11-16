import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select

from app.messaging import DEAD_QUEUE, RETRY_QUEUES
from app.models import Outbox, Payment, PaymentStatus
from app.processor import PaymentProcessor
from app.schemas import PaymentEvent

pytestmark = pytest.mark.integration


def event_for(payment, attempt=1):
    return PaymentEvent(event_id=uuid4(), payment_id=payment.id, attempt=attempt)


@pytest.mark.parametrize("status", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_duplicate_events_do_not_repeat_payment_or_webhook(
    database, payment, gateway, status
):
    gateway.status = status
    requests = []

    async def webhook(request):
        requests.append(request)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(database, gateway, client)
        event = event_for(payment)
        await asyncio.gather(processor.handle(event), processor.handle(event))
        await processor.handle(event)
    assert gateway.calls == len(requests) == 1
    assert requests[0].headers["Idempotency-Key"] == str(payment.id)
    assert json.loads(requests[0].content)["status"] == status
    async with database.sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.status == status
        assert saved.processed_at is not None
        assert saved.webhook_delivered_at is not None
        assert saved.delivery_attempts == 1


async def test_webhook_succeeds_on_third_attempt_without_recharging(database, payment, gateway):
    calls = 0

    async def webhook(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200 if calls == 3 else 503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(database, gateway, client)
        for attempt in (1, 1, 2, 2, 3, 3):
            await processor.handle(event_for(payment, attempt))
    assert gateway.calls == 1
    assert calls == 3
    async with database.sessions() as session:
        saved = await session.get(Payment, payment.id)
        assert saved.delivery_attempts == 3
        assert saved.webhook_delivered_at is not None
        routes = list(await session.scalars(select(Outbox.routing_key)))
        assert sorted(routes) == sorted(["payments.new", *RETRY_QUEUES.values()])


@pytest.mark.parametrize("failure", ["http", "timeout"])
async def test_third_failure_creates_dlq_event_atomically(database, payment, gateway, failure):
    calls = 0

    async def webhook(request):
        nonlocal calls
        calls += 1
        if failure == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        processor = PaymentProcessor(database, gateway, client)
        for attempt in (1, 2, 3, 3):
            await processor.handle(event_for(payment, attempt))
    assert gateway.calls == 1
    assert calls == 3
    async with database.sessions() as session:
        saved = await session.get(Payment, payment.id)
        dead = (await session.scalars(select(Outbox).where(Outbox.routing_key == DEAD_QUEUE))).one()
        assert saved.delivery_failed_at is not None
        assert saved.webhook_delivered_at is None
        assert saved.status == PaymentStatus.SUCCEEDED
        assert dead.payload["attempt"] == saved.delivery_attempts == 3
        assert "error" in dead.payload


async def test_restart_after_payment_commit_does_not_recharge(database, payment, gateway):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        event = event_for(payment)
        await PaymentProcessor(database, gateway, client).process_payment(event)
        # Новый экземпляр имитирует рестарт между коммитом платежа и отправкой HTTP.
        await PaymentProcessor(database, gateway, client).handle(event)
    assert gateway.calls == 1
