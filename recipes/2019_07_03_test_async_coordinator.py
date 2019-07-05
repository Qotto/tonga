import asyncio
import functools
import uuid
import uvloop
from asyncio import Task
from random import randint
from typing import Dict, Any
import time

from tonga.services.coordinator.async_coordinator import async_coordinator


class FailToFindKey(KeyError):
    pass


class DBIsLocked(Exception):
    pass


async def send_store_record(record: str):
     # print('Send record : ', record)
    return 'ack'


class MicroDB:
    _db: Dict[str, bytes]
    _lock: Dict[str, bool]
    _communicator: async_coordinator.AsyncCoordinator

    def __init__(self):
        self._db = dict()
        self._lock = dict()
        self._communicator = async_coordinator.AsyncCoordinator(maxsize=0, loop=asyncio.get_event_loop())

    def __lock_db(self, key: str) -> None:
        self._communicator.put_nowait(key, True)
        self._lock[key] = True

    def __unlock_db(self, key: str, f: Task) -> None:
        self._communicator.put_nowait(key, False)

    async def get_lock(self) -> Dict[str, bool]:
        return self._lock.copy()

    async def __store_set(self, key: str, value: bytes):
        # print(f'Start store set {key}')
        self._db[key] = value
        # print(f'End store set {key}')

    async def __store_del(self, key: str):
        # print(f'Start store del {key}')
        del self._db[key]
        # print(f'End store del {key}')

    async def set(self, key: str, value: bytes) -> str:
        await send_store_record(key + '-' + value.decode('utf-8'))
        self.__lock_db(key)
        fu = asyncio.ensure_future(self.__store_set(key, value), loop=asyncio.get_event_loop())
        fu.add_done_callback(functools.partial(self.__unlock_db, key))
        return 'ack'

    async def delete(self, key: str) -> str:
        await send_store_record(key + '-' + 'del')
        self.__lock_db(key)
        fu = asyncio.ensure_future(self.__store_del(key), loop=asyncio.get_event_loop())
        fu.add_done_callback(functools.partial(self.__unlock_db, key))
        return 'ack'

    async def get(self, key: str) -> bytes:
        await self.__ready(key)
        try:
            # print(self._communicator.get_dict_copy())
            return self._db[key]
        except KeyError:
            raise FailToFindKey

    async def __ready(self, key) -> None:
        try:
            if self._lock[key]:
                # print('In ready func for key: ', key)
                self._lock[key] = await self._communicator.get(key)
                # print('End lock = ', self._lock[key])
        except KeyError:
            raise FailToFindKey


class StoreRecord:
    _key: str
    _value: bytes

    def __init__(self, key: str, value: bytes):
        self._key = key
        self._value = value

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._key

    def to_dict(self) -> Dict[str, Any]:
        return {'key': self._key, 'value': self._value}

    @classmethod
    def from_dict(cls, data):
        return cls(data['key'], data['value'])


async def main():
    # Create a "cancel_me" Task
    micro_db = MicroDB()

    # print(f'Dev try to get record...')
    #
    # try:
    #     await micro_db.get('test')
    # except FailToFindKey:
    #     print('Logic')
    #
    # print(f'Dev send record')
    # r1 = StoreRecord(uuid.uuid4().hex, b'value1')
    # r2 = StoreRecord(uuid.uuid4().hex, b'value2')
    # r3 = StoreRecord(uuid.uuid4().hex, b'value3')
    # r4 = StoreRecord(uuid.uuid4().hex, b'value4')
    #
    # res = await asyncio.gather(asyncio.create_task(micro_db.set(**r1.to_dict())),
    #                            asyncio.create_task(micro_db.set(**r2.to_dict())),
    #                            asyncio.create_task(micro_db.set(**r3.to_dict())),
    #                            asyncio.create_task(micro_db.set(**r4.to_dict())))
    #
    # print('Dev receive for ack1: ', res)
    #
    # rel = await asyncio.gather(asyncio.create_task(micro_db.get(r1.key)),
    #                            asyncio.create_task(micro_db.get(r2.key)),
    #                            asyncio.create_task(micro_db.get(r3.key)),
    #                            asyncio.create_task(micro_db.get(r4.key)))
    #
    # print('Dev get r1-r4 : ', rel)
    #
    # rel = await asyncio.gather(asyncio.create_task(micro_db.delete(r1.key)),
    #                            asyncio.create_task(micro_db.delete(r2.key)),
    #                            asyncio.create_task(micro_db.delete(r3.key)),
    #                            asyncio.create_task(micro_db.delete(r4.key)))
    #
    # print('Dev get r1-r4 : ', rel)
    #
    # try:
    #     await micro_db.get(r1.key)
    # except FailToFindKey:
    #     print('Logic R1')
    #
    # try:
    #     await micro_db.get(r2.key)
    # except FailToFindKey:
    #     print('Logic R2')
    #
    # try:
    #     await micro_db.get(r3.key)
    # except FailToFindKey:
    #     print('Logic R3')
    #
    # try:
    #     await micro_db.get(r4.key)
    # except FailToFindKey:
    #     print('Logic R4')

    tic = time.clock()
    records = list()
    for i in range(0, 1000000):
        records.append(StoreRecord(uuid.uuid4().hex, b'value-' + str(i).encode('utf-8')))

    ret2 = await asyncio.gather(*[asyncio.create_task(micro_db.set(**record.to_dict())) for record in records])

    # print(ret2)

    ret3 = await asyncio.gather(*[asyncio.create_task(micro_db.get(record.key)) for record in records])

    toc = time.clock()

    print('Return for 1000000 record : ', toc - tic)
    # (ret3)

loop = uvloop.new_event_loop()
asyncio.set_event_loop(loop)
asyncio.run(main())
