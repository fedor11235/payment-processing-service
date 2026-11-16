from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.messaging import enqueue
from app.models import Payment, PaymentStatus
from app.schemas import PaymentCreate


class IdempotencyConflict(Exception):
    pass


async def create_payment(
    session: AsyncSession, request: PaymentCreate, idempotency_key: str
) -> Payment:
    fingerprint = request.fingerprint()
    async with session.begin():
        payment = await session.scalar(
            insert(Payment)
            .values(
                id=uuid4(),
                amount=request.amount,
                currency=request.currency,
                description=request.description,
                metadata_=request.metadata,
                webhook_url=str(request.webhook_url),
                idempotency_key=idempotency_key,
                request_hash=fingerprint,
                status=PaymentStatus.PENDING,
                delivery_attempts=0,
            )
            .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
            .returning(Payment)
        )
        if payment is not None:
            enqueue(session, payment.id)
        else:
            payment = (
                await session.scalars(
                    select(Payment).where(Payment.idempotency_key == idempotency_key)
                )
            ).one()
            if payment.request_hash != fingerprint:
                raise IdempotencyConflict
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment | None:
    return await session.get(Payment, payment_id)
