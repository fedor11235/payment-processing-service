import asyncio
import logging
from datetime import UTC, datetime, timedelta

from faststream.rabbit import RabbitBroker
from sqlalchemy import func, select

from app.database import Database
from app.messaging import DEAD_EXCHANGE, DEAD_QUEUE, EVENTS_EXCHANGE, declare_topology
from app.models import Outbox

logger = logging.getLogger(__name__)


class OutboxPublisher:
    def __init__(self, database: Database, broker: RabbitBroker, poll_interval: float = 0.5):
        self.database = database
        self.broker = broker
        self.poll_interval = poll_interval

    async def publish_batch(self) -> int:
        async with self.database.sessions() as session, session.begin():
            events = (
                await session.scalars(
                    select(Outbox)
                    .where(Outbox.published_at.is_(None), Outbox.available_at <= func.now())
                    .order_by(Outbox.available_at, Outbox.id)
                    .with_for_update(skip_locked=True)
                    .limit(20)
                )
            ).all()
            for event in events:
                event.publish_attempts += 1
                try:
                    await self.broker.publish(
                        event.payload,
                        exchange=DEAD_EXCHANGE
                        if event.routing_key == DEAD_QUEUE
                        else EVENTS_EXCHANGE,
                        routing_key=event.routing_key,
                        message_id=str(event.id),
                        persist=True,
                        mandatory=True,
                        timeout=5,
                    )
                except Exception as exc:
                    # Не теряем событие при недоступном брокере или отсутствии binding.
                    event.last_error = type(exc).__name__
                    event.available_at = datetime.now(UTC) + timedelta(
                        seconds=2 ** min(event.publish_attempts, 6)
                    )
                    logger.warning("Публикация отложена: event_id=%s", event.id)
                else:
                    event.published_at = datetime.now(UTC)
                    event.last_error = None
            return len(events)

    async def run(self) -> None:
        connected = False
        try:
            while True:
                try:
                    if not connected:
                        await self.broker.connect()
                        await declare_topology(self.broker)
                        connected = True
                    await self.publish_batch()
                except Exception:
                    logger.exception("Ошибка цикла outbox; повтор после паузы")
                    connected = False
                await asyncio.sleep(self.poll_interval)
        finally:
            await self.broker.stop()
