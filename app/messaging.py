from uuid import UUID, uuid4

from faststream.rabbit import Channel, RabbitBroker, RabbitExchange, RabbitQueue
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Outbox
from app.schemas import PaymentEvent

MAIN_QUEUE = "payments.new"
DEAD_QUEUE = "payments.dead"
RETRY_QUEUES = {2: "payments.retry.1", 3: "payments.retry.2"}
EVENTS_EXCHANGE = RabbitExchange("payments.events", durable=True)
DEAD_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    MAIN_QUEUE,
    durable=True,
    arguments={
        "x-dead-letter-exchange": DEAD_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_QUEUE,
    },
)


def make_broker(url: str) -> RabbitBroker:
    return RabbitBroker(
        url,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
        graceful_timeout=20,
    )


async def declare_topology(broker: RabbitBroker) -> None:
    events = await broker.declare_exchange(EVENTS_EXCHANGE)
    dead = await broker.declare_exchange(DEAD_EXCHANGE)
    queue = await broker.declare_queue(PAYMENTS_QUEUE)
    await queue.bind(events, routing_key=MAIN_QUEUE)
    dlq = await broker.declare_queue(RabbitQueue(DEAD_QUEUE, durable=True))
    await dlq.bind(dead, routing_key=DEAD_QUEUE)
    for attempt, name in RETRY_QUEUES.items():
        retry = await broker.declare_queue(
            RabbitQueue(
                name,
                durable=True,
                arguments={
                    "x-message-ttl": 1000 * 2 ** (attempt - 2),
                    "x-dead-letter-exchange": EVENTS_EXCHANGE.name,
                    "x-dead-letter-routing-key": MAIN_QUEUE,
                },
            )
        )
        await retry.bind(events, routing_key=name)


def enqueue(
    session: AsyncSession,
    payment_id: UUID,
    *,
    attempt: int = 1,
    routing_key: str = MAIN_QUEUE,
    error: str | None = None,
) -> Outbox:
    event_id = uuid4()
    payload = PaymentEvent(event_id=event_id, payment_id=payment_id, attempt=attempt).model_dump(
        mode="json"
    )
    if error is not None:
        payload["error"] = error
    event = Outbox(id=event_id, payload=payload, routing_key=routing_key)
    session.add(event)
    return event
