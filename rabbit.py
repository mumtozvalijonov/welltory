import asyncio
import aio_pika
from aio_pika.pool import Pool
import ujson

import config


async def get_connection():
    return await aio_pika.connect_robust(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        login=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASSWORD
    )


def get_channel_pool():
    connection_pool = Pool(get_connection, max_size=2)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    channel_pool = Pool(get_channel, max_size=10)
    return channel_pool


async def publish(channel_pool, payload: str, queue_name):
    async with channel_pool.acquire() as channel:
        await channel.declare_queue(queue_name, auto_delete=True)
        await channel.default_exchange.publish(
            aio_pika.Message(payload.encode()),
            queue_name
        )
