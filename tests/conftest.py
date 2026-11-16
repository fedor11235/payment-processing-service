import os
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text

from app.api import create_app
from app.config import Settings
from app.database import Database
from app.messaging import declare_topology, make_broker
from app.models import Base, PaymentStatus
from app.payments import create_payment
from app.schemas import PaymentCreate


@pytest.fixture
def payment_request():
    return PaymentCreate(
        amount="100.00",
        currency="RUB",
        description="Заказ 42",
        metadata={"order_id": 42},
        webhook_url="http://webhook.test/result",
    )


@pytest.fixture
async def database():
    url = os.getenv("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Укажите TEST_DATABASE_URL для интеграционных тестов")
    schema = f"test_{uuid4().hex}"
    admin = Database(url)
    async with admin.engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    db = Database(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with db.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        yield db
    finally:
        await db.close()
        async with admin.engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.close()


@pytest.fixture
async def client(database):
    settings = Settings(api_key="test-key")
    app = create_app(settings, run_outbox=False, database=database)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": "test-key"},
    ) as client:
        yield client


@pytest.fixture
async def payment(database, payment_request):
    async with database.sessions() as session:
        return await create_payment(session, payment_request, "test-payment")


class StubGateway:
    def __init__(self, status=PaymentStatus.SUCCEEDED):
        self.status = status
        self.calls = 0

    async def charge(self, payment_id):
        self.calls += 1
        return self.status


@pytest.fixture
def gateway():
    return StubGateway()


@pytest.fixture
async def broker():
    url = os.getenv("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Укажите TEST_RABBITMQ_URL для тестов RabbitMQ")
    parts = urlsplit(url)
    username, password = parts.username or "guest", parts.password or "guest"
    management_url = os.getenv("TEST_RABBITMQ_MANAGEMENT_URL", "http://localhost:15672")
    vhost = f"test_{uuid4().hex}"
    async with httpx.AsyncClient(base_url=management_url, auth=(username, password)) as management:
        response = await management.put(f"/api/vhosts/{vhost}")
        response.raise_for_status()
        try:
            response = await management.put(
                f"/api/permissions/{vhost}/{quote(username, safe='')}",
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
            response.raise_for_status()
            broker = make_broker(urlunsplit(parts._replace(path=f"/{vhost}")))
            try:
                await broker.connect()
                await declare_topology(broker)
                yield broker
            finally:
                await broker.stop()
        finally:
            response = await management.delete(f"/api/vhosts/{vhost}")
            response.raise_for_status()
