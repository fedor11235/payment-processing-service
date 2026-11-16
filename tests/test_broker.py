import asyncio
import json
from time import monotonic

import httpx
import pytest
from faststream.rabbit import RabbitQueue
from sqlalchemy import select

from app.messaging import DEAD_QUEUE, EVENTS_EXCHANGE, PAYMENTS_QUEUE
from app.models import Outbox, Payment, PaymentStatus
from app.outbox import OutboxPublisher
from app.processor import PaymentProcessor
from app.worker import register_consumer

pytestmark = pytest.mark.integration


async def get_message(queue, wait_seconds=5):
    async with asyncio.timeout(wait_seconds):
        while True:
            message = await queue.get(fail=False)
            if message is not None:
                return message
            await asyncio.sleep(0.05)


async def test_outbox_publishes_persistent_message_to_real_rabbitmq(database, payment, broker):
    await OutboxPublisher(database, broker).publish_batch()
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    message = await get_message(queue)
    assert json.loads(message.body)["payment_id"] == str(payment.id)
    assert message.delivery_mode == 2
    await message.ack()


async def test_unroutable_message_is_not_marked_published(database, payment, broker):
    async with database.sessions() as session, session.begin():
        event = (await session.scalars(select(Outbox))).one()
        event.routing_key = "missing.binding"
    await OutboxPublisher(database, broker).publish_batch()
    async with database.sessions() as session:
        event = (await session.scalars(select(Outbox))).one()
        assert event.published_at is None
        assert event.publish_attempts == 1
        assert event.last_error is not None


@pytest.mark.parametrize(
    "webhook_ok,business_status",
    [
        (True, PaymentStatus.SUCCEEDED),
        (True, PaymentStatus.FAILED),
        (False, PaymentStatus.SUCCEEDED),
    ],
)
async def test_consumer_pipeline_with_real_retry_queues(
    database,
    payment,
    broker,
    gateway,
    webhook_ok,
    business_status,
):
    gateway.status = business_status
    timestamps = []

    async def webhook(request):
        timestamps.append(monotonic())
        return httpx.Response(200 if webhook_ok else 503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(webhook)) as client:
        register_consumer(broker, PaymentProcessor(database, gateway, client))
        await broker.start()
        publisher = OutboxPublisher(database, broker)
        async with asyncio.timeout(15):
            while True:
                await publisher.publish_batch()
                async with database.sessions() as session:
                    saved = await session.get(Payment, payment.id)
                    if saved.webhook_delivered_at or saved.delivery_failed_at:
                        break
                await asyncio.sleep(0.05)
        await publisher.publish_batch()
        assert gateway.calls == 1
        assert saved.status == business_status
        assert len(timestamps) == saved.delivery_attempts == (1 if webhook_ok else 3)
        if not webhook_ok:
            assert timestamps[1] - timestamps[0] >= 0.9
            assert timestamps[2] - timestamps[1] >= 1.9
            dlq = await broker.declare_queue(RabbitQueue(DEAD_QUEUE, durable=True))
            message = await get_message(dlq)
            assert json.loads(message.body)["payment_id"] == str(payment.id)
            assert json.loads(message.body)["attempt"] == 3
            await message.ack()


async def test_invalid_json_goes_to_dlq_without_blocking_consumer(database, broker, gateway):
    async with httpx.AsyncClient() as client:
        register_consumer(broker, PaymentProcessor(database, gateway, client))
        await broker.start()
        await broker.publish(
            b"{broken-json",
            exchange=EVENTS_EXCHANGE,
            routing_key=PAYMENTS_QUEUE.name,
            content_type="application/json",
            persist=True,
        )
        dlq = await broker.declare_queue(RabbitQueue(DEAD_QUEUE, durable=True))
        message = await get_message(dlq)
        assert message.body == b"{broken-json"
        assert gateway.calls == 0
        await message.ack()
