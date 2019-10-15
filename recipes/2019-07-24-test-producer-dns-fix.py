import asyncio

from aiokafka import AIOKafkaProducer
import functools
import time


def on_done(i):
    print('Send msg n°', i)


async def produce(mloop):
    producer = AIOKafkaProducer(
        loop=mloop, bootstrap_servers='infra-cp-kafka')

    await producer.start()
    try:
        # Produce message
        for i in range(0, 100):
            coro = await producer.send_and_wait("my_favorite_topic", b"Super message")
            print(coro)
            time.sleep(0.4)
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    loop.run_until_complete(produce(loop))
