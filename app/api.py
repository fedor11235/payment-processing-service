import asyncio
import hmac
from contextlib import asynccontextmanager, suppress
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.security import APIKeyHeader
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings
from app.database import Database
from app.messaging import make_broker
from app.outbox import OutboxPublisher
from app.payments import IdempotencyConflict, create_payment, get_payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def create_app(
    settings: Settings | None = None, *, run_outbox: bool = True, database: Database | None = None
) -> FastAPI:
    settings = settings or Settings()
    database = database or Database(settings.database_url)

    async def authenticate(key: Annotated[str | None, Depends(api_key_header)]):
        if key is None or not hmac.compare_digest(
            key.encode(), settings.api_key.get_secret_value().encode()
        ):
            raise HTTPException(status_code=401, detail="Неверный API ключ")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = None
        if run_outbox:
            publisher = OutboxPublisher(
                database, make_broker(settings.rabbitmq_url), settings.outbox_poll_interval
            )
            task = asyncio.create_task(publisher.run(), name="outbox-publisher")
        try:
            yield
        finally:
            if task is not None:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
            await database.close()

    app = FastAPI(
        title="Асинхронный процессинг платежей",
        version="1.0.0",
        lifespan=lifespan,
        dependencies=[Depends(authenticate)],
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.database = database

    async def session_dependency(request: Request):
        async with request.app.state.database.sessions() as session:
            yield session

    Session = Annotated[AsyncSession, Depends(session_dependency)]

    @app.post(
        "/api/v1/payments",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=PaymentAccepted,
        responses={409: {"description": "Ключ уже использован"}},
    )
    async def post_payment(
        body: PaymentCreate,
        session: Session,
        idempotency_key: Annotated[str, Header(min_length=1, max_length=255)],
    ):
        if not idempotency_key.strip():
            raise HTTPException(status_code=422, detail="Idempotency-Key не должен быть пустым")
        try:
            payment = await create_payment(session, body, idempotency_key)
        except IdempotencyConflict as exc:
            raise HTTPException(
                status_code=409, detail="Ключ использован с другим запросом"
            ) from exc
        return PaymentAccepted(
            payment_id=payment.id, status=payment.status, created_at=payment.created_at
        )

    @app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
    async def read_payment(payment_id: UUID, session: Session):
        payment = await get_payment(session, payment_id)
        if payment is None:
            raise HTTPException(status_code=404, detail="Платеж не найден")
        return PaymentDetail(
            payment_id=payment.id,
            status=payment.status,
            created_at=payment.created_at,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            webhook_url=payment.webhook_url,
            idempotency_key=payment.idempotency_key,
            processed_at=payment.processed_at,
            webhook_delivered_at=payment.webhook_delivered_at,
            delivery_failed_at=payment.delivery_failed_at,
            delivery_attempts=payment.delivery_attempts,
        )

    @app.get("/health", include_in_schema=False)
    async def health(session: Session):
        await session.execute(text("SELECT 1"))
        return {"status": "ok"}

    @app.get("/openapi.json", include_in_schema=False)
    async def openapi():
        return app.openapi()

    @app.get("/docs", include_in_schema=False)
    async def docs():
        # Передайте X-API-Key также при загрузке /docs и /openapi.json.
        return get_swagger_ui_html(openapi_url="/openapi.json", title="Документация API")

    return app


app = create_app()
