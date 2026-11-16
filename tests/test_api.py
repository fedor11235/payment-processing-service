import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment
from app.payments import create_payment

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(
    "path", ["/health", "/openapi.json", "/docs", "/api/v1/payments/" + str(uuid4())]
)
async def test_all_endpoints_require_api_key(client, path):
    assert (await client.get(path, headers={"X-API-Key": "wrong"})).status_code == 401
    client.headers.pop("X-API-Key")
    assert (await client.get(path)).status_code == 401


async def test_create_and_read_payment(client, database, payment_request):
    response = await client.post(
        "/api/v1/payments",
        json=payment_request.model_dump(mode="json"),
        headers={"Idempotency-Key": "order-42"},
    )
    assert response.status_code == 202
    accepted = response.json()
    assert accepted["status"] == "pending"
    detail = await client.get(f"/api/v1/payments/{accepted['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["amount"] == "100.00"
    assert detail.json()["metadata"] == {"order_id": 42}
    assert detail.json()["processed_at"] is None
    async with database.sessions() as session:
        event = (await session.scalars(select(Outbox))).one()
        assert event.payload["payment_id"] == accepted["payment_id"]
        assert event.published_at is None


async def test_idempotency_replay_and_conflict(client, payment_request):
    body = payment_request.model_dump(mode="json")
    headers = {"Idempotency-Key": "same"}
    first = await client.post("/api/v1/payments", json=body, headers=headers)
    second = await client.post(
        "/api/v1/payments", json={**body, "amount": "100.0"}, headers=headers
    )
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    conflict = await client.post(
        "/api/v1/payments", json={**body, "amount": "101"}, headers=headers
    )
    assert conflict.status_code == 409


async def test_concurrent_requests_create_one_payment_and_event(client, database, payment_request):
    responses = await asyncio.gather(
        *[
            client.post(
                "/api/v1/payments",
                json=payment_request.model_dump(mode="json"),
                headers={"Idempotency-Key": "parallel"},
            )
            for _ in range(12)
        ]
    )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["payment_id"] for r in responses}) == 1
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


async def test_missing_key_validation_and_not_found(client, payment_request):
    body = payment_request.model_dump(mode="json")
    assert (await client.post("/api/v1/payments", json=body)).status_code == 422
    assert (
        await client.post("/api/v1/payments", json=body, headers={"Idempotency-Key": " "})
    ).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_payment_and_outbox_rollback_together(database, payment_request, monkeypatch):
    def broken_enqueue(*args, **kwargs):
        raise RuntimeError("Искусственная ошибка записи события")

    monkeypatch.setattr("app.payments.enqueue", broken_enqueue)
    async with database.sessions() as session:
        with pytest.raises(RuntimeError):
            await create_payment(session, payment_request, "rollback")
    async with database.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 0
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 0
