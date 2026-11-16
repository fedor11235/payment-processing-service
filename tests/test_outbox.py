import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from app.models import Outbox
from app.outbox import OutboxPublisher

pytestmark = pytest.mark.integration


class PublisherStub:
    def __init__(self, fail=False):
        self.fail = fail
        self.sent = []

    async def publish(self, payload, **kwargs):
        await asyncio.sleep(0)
        if self.fail:
            raise ConnectionError("Брокер недоступен")
        self.sent.append((payload, kwargs))


async def test_failed_publication_remains_pending_then_recovers(database, payment):
    broker = PublisherStub(fail=True)
    publisher = OutboxPublisher(database, broker)
    assert await publisher.publish_batch() == 1
    async with database.sessions() as session, session.begin():
        row = (await session.scalars(select(Outbox))).one()
        assert row.published_at is None
        assert row.last_error == "ConnectionError"
        assert row.publish_attempts == 1
        assert row.available_at > datetime.now(UTC)
        row.available_at = datetime.now(UTC)
    broker.fail = False
    await publisher.publish_batch()
    async with database.sessions() as session:
        row = (await session.scalars(select(Outbox))).one()
        assert row.published_at is not None
        assert row.publish_attempts == 2
        assert row.last_error is None
    assert len(broker.sent) == 1
    assert broker.sent[0][1]["persist"] is True
    assert broker.sent[0][1]["mandatory"] is True


async def test_parallel_publishers_skip_locked_rows(database, payment):
    broker = PublisherStub()
    await asyncio.gather(
        OutboxPublisher(database, broker).publish_batch(),
        OutboxPublisher(database, broker).publish_batch(),
    )
    assert len(broker.sent) == 1
