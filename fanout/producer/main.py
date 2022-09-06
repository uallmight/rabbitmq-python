import asyncio
import uuid
from aio_pika import (
    connect_robust,
    RobustChannel,
    RobustConnection,
    RobustExchange,
    ExchangeType,
    Message
)

async def _main():
    conn: RobustConnection = await connect_robust(
        host="localhost",
        port=5672,
        login="guest",
        password="guest",
    )

    ch: RobustChannel = await conn.channel()
    ex: RobustExchange = await ch.declare_exchange("schoeder.topic", type=ExchangeType.TOPIC)
    for i in range(0, 10000):
        await ex.publish(Message(
            body=f"Hello world {i}!".encode("utf8"),
            message_id=str(uuid.uuid4()),
            content_encoding="utf8",
            content_type="application/text",
            app_id="publisher",
        ), routing_key="resource.create")
        print (f"published message {i}")
        # await asyncio.sleep(0.500)

    print ("done")

if __name__ == "__main__":
    asyncio.run(_main())    