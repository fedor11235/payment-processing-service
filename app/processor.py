import asyncio
import logging
import random
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import Database
from app.messaging import DEAD_QUEUE, RETRY_QUEUES, enqueue
from app.models import Payment, PaymentStatus
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


class Gateway(Protocol):
    async def charge(self, payment_id: UUID) -> PaymentStatus: ...


class EmulatedGateway:
    def __init__(self, settings: Settings):
        self.settings = settings

    async def charge(self, payment_id: UUID) -> PaymentStatus:
        await asyncio.sleep(
            random.uniform(self.settings.gateway_delay_min, self.settings.gateway_delay_max)
        )
        return (
            PaymentStatus.SUCCEEDED
            if random.random() < self.settings.gateway_success_rate
            else PaymentStatus.FAILED
        )


class MissingPayment(Exception):
    pass


async def lock_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    payment = await session.scalar(
        select(Payment).where(Payment.id == payment_id).with_for_update()
    )
    if payment is None:
        raise MissingPayment(str(payment_id))
    return payment


def already_handled(payment: Payment, event: PaymentEvent) -> bool:
    return bool(
        payment.webhook_delivered_at is not None
        or payment.delivery_failed_at is not None
        or event.attempt <= payment.delivery_attempts
    )


def webhook_payload(payment: Payment) -> dict:
    return {
        "event_id": str(payment.id),
        "event_type": "payment.processed",
        "payment_id": str(payment.id),
        "status": payment.status,
        "amount": format(payment.amount, ".2f"),
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.metadata_,
        "created_at": payment.created_at.isoformat(),
        "processed_at": payment.processed_at.isoformat(),
    }


class PaymentProcessor:
    def __init__(self, database: Database, gateway: Gateway, client: httpx.AsyncClient):
        self.database = database
        self.gateway = gateway
        self.client = client

    async def handle(self, event: PaymentEvent) -> None:
        try:
            await self.process_payment(event)
            await self.deliver_webhook(event)
        except MissingPayment:
            raise
        except Exception as exc:
            await self.record_failure(event, exc)

    async def process_payment(self, event: PaymentEvent) -> None:
        async with self.database.sessions() as session, session.begin():
            payment = await lock_payment(session, event.payment_id)
            if already_handled(payment, event) or payment.status != PaymentStatus.PENDING:
                return
            payment.status = await self.gateway.charge(payment.id)
            payment.processed_at = datetime.now(UTC)
        # Коммит результата до HTTP: повтор webhook не запускает шлюз заново.

    async def deliver_webhook(self, event: PaymentEvent) -> None:
        async with self.database.sessions() as session, session.begin():
            payment = await lock_payment(session, event.payment_id)
            if already_handled(payment, event):
                return
            response = await self.client.post(
                payment.webhook_url,
                json=webhook_payload(payment),
                headers={"Idempotency-Key": str(payment.id), "X-Event-ID": str(payment.id)},
            )
            response.raise_for_status()
            payment.webhook_delivered_at = datetime.now(UTC)
            payment.delivery_attempts = event.attempt
            logger.info("Webhook доставлен: payment_id=%s", payment.id)

    async def record_failure(self, event: PaymentEvent, exc: Exception) -> None:
        error = type(exc).__name__
        if isinstance(exc, httpx.HTTPStatusError):
            error += f": HTTP {exc.response.status_code}"
        async with self.database.sessions() as session, session.begin():
            payment = await lock_payment(session, event.payment_id)
            if already_handled(payment, event):
                return
            payment.delivery_attempts = event.attempt
            if event.attempt < 3:
                enqueue(
                    session,
                    payment.id,
                    attempt=event.attempt + 1,
                    routing_key=RETRY_QUEUES[event.attempt + 1],
                )
            else:
                payment.delivery_failed_at = datetime.now(UTC)
                enqueue(
                    session, payment.id, attempt=event.attempt, routing_key=DEAD_QUEUE, error=error
                )
            logger.warning(
                "Ошибка обработки: payment_id=%s attempt=%s error=%s",
                payment.id,
                event.attempt,
                error,
            )
