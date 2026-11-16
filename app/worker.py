import asyncio
import logging

import httpx
from faststream import AckPolicy
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError

from app.config import Settings
from app.database import Database
from app.messaging import EVENTS_EXCHANGE, PAYMENTS_QUEUE, declare_topology, make_broker
from app.processor import EmulatedGateway, MissingPayment, PaymentProcessor
from app.schemas import PaymentEvent

logger = logging.getLogger(__name__)


async def run() -> None:
    settings = Settings()
    database = Database(settings.database_url)
    broker = make_broker(settings.rabbitmq_url)
    async with httpx.AsyncClient(
        timeout=settings.webhook_timeout, follow_redirects=False
    ) as client:
        processor = PaymentProcessor(database, EmulatedGateway(settings), client)

        @broker.subscriber(PAYMENTS_QUEUE, EVENTS_EXCHANGE, ack_policy=AckPolicy.MANUAL)
        async def consume(body: dict, message: RabbitMessage):
            try:
                event = PaymentEvent.model_validate(body)
            except ValidationError:
                logger.warning("Некорректное событие отправлено в DLQ")
                await message.reject(requeue=False)
                return
            try:
                await processor.handle(event)
            except MissingPayment:
                await message.reject(requeue=False)
            except Exception:
                # Без доступной БД нельзя надежно записать попытку и retry в outbox.
                logger.exception("Ошибка инфраструктуры; сообщение возвращено в очередь")
                await asyncio.sleep(1)
                await message.nack(requeue=True)
            else:
                # Retry/DLQ уже сохранены транзакционно до подтверждения сообщения.
                await message.ack()

        try:
            await broker.connect()
            await declare_topology(broker)
            await broker.start()
            await asyncio.Event().wait()
        finally:
            await broker.stop()
            await database.close()


if __name__ == "__main__":
    import signal

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    async def main():
        task = asyncio.create_task(run())
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(main())
