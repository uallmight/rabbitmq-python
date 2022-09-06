import asyncio
from aio_pika import (
    connect_robust,
    RobustConnection,
    RobustChannel,
    RobustExchange,
    ExchangeType,
    RobustQueue,
    IncomingMessage
)

def _consume(msg: IncomingMessage):
    print (str(msg.body))


async def _main():
    conn: RobustConnection = await connect_robust(
        host="localhost",
        port=5672,
        login="guest",
        password="guest"
    )
    ch: RobustChannel = await conn.channel()
    ex: RobustExchange = await ch.declare_exchange("schoeder.topic", type=ExchangeType.TOPIC)
    queue: RobustQueue = await ch.declare_queue(exclusive=True)
    await queue.bind(exchange=ex, routing_key="resource.create")
    
    await queue.consume(_consume)

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        ch.close()
        conn.close()


if __name__ == "__main__":
    asyncio.run(_main())
    