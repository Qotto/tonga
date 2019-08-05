import asyncio
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaClient
from aiokafka.conn import AIOKafkaConnection
from aiokafka.cluster import ClusterMetadata
from aiokafka.errors import ConnectionError
from kafka.errors import KafkaError

from typing import Dict


def is_connected(conns: Dict[str, AIOKafkaConnection]):
    ok = 0

    for conn in conns.items():
        if conn[1].connected():
            ok += 1

    if ok == 0:
        return False
    return True


async def check_if_kafka_is_alive(my_client: AIOKafkaClient):
    while 1:
        conns: Dict[str, AIOKafkaConnection] = my_client.__getattribute__('_conns')

        print(my_client._bootstrap_servers)

        print('Host = ', my_client.hosts)
        if not is_connected(conns):
            print('RENEW CONNECTION')
            try:
                my_client.__setattr__('cluster', ClusterMetadata(metadata_max_age_ms=300000))
                my_client.__setattr__('_topics', set())
                my_client.__setattr__('_conns', {})
                my_client.__setattr__('_sync_task', None)

                loop = asyncio.get_event_loop()
                my_client.__setattr__('_md_update_fut', None)
                my_client.__setattr__('_md_update_waiter', loop.create_future())
                my_client.__setattr__('_get_conn_lock',  asyncio.Lock(loop=loop))

                await my_client.bootstrap()
            except ConnectionError:
                pass
        else:
            for conn in conns.items():
                print(conn)
                print(conn[1].connected())
                try:
                    if not conn[1].connected():
                        print('TRY RE CONNECTION')
                        await conn[1].connect()
                        if not conn[1].connected():
                            print('RENEW CONNECTION')
                            await my_client.bootstrap()
                except Exception as err:
                    print(err)
        await asyncio.sleep(1)


async def consume():
    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer('my_favorite_topic', bootstrap_servers='infra-cp-kafka', auto_offset_reset='earliest',
                                loop=loop)

    await consumer.start()

    # client = consumer.__getattribute__('_client')

    # asyncio.ensure_future(check_if_kafka_is_alive(client))

    try:
        async for msg in consumer:
            print(msg)
    except KafkaError as err:
        print(err)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    asyncio.ensure_future(consume())

    try:
        loop.run_forever()
    except Exception:
        loop.stop()


