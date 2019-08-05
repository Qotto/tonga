import asyncio
from asyncio import AbstractEventLoop
from typing import List, Dict


class BaseRecord:
    def __init__(self, namespace: str, i: int):
        self.namespace = namespace
        self.i = i


class BaseProducer:
    def __init__(self):
        ...

    async def stop(self):
        print('Producer stopped')

    async def start(self):
        print('Consumer started')

    async def send_and_wait(self, record: BaseRecord, topic: str):
        print(f'Send ans wait record {record.namespace} to {topic}')

        if topic == 'coffee-machine-commands':
            record.namespace = 'coffee-machine-results'
            coffee_machine_result.append(record)

        elif topic == 'bartender-events':
            bartender_event_records.append(record)


class BaseConsumer:
    offset: Dict[str, int]

    def __init__(self, topics: List[str]):
        self.offset = dict()

        for topic in topics:
            self.offset[topic] = 0

    async def stop(self):
        print('Consumer stopped')

    async def start(self):
        print('Consumer started')

    async def get_one(self, topic: str):

        if topic == 'cash-register-events':
            if self.offset[topic] < len(bills_records):
                self.offset[topic] += 1
                return bills_records[self.offset[topic] - 1]

        elif topic == 'coffee-machine-results':
            if self.offset[topic] < len(coffee_machine_result):
                self.offset[topic] += 1
                return coffee_machine_result[self.offset[topic] - 1]


class Stream:
    _topic: str
    _producer: BaseProducer
    _consumer: BaseConsumer
    _ctx_manager: int

    def __init__(self, topic: str, producer: BaseProducer, consumer: BaseConsumer, loop: AbstractEventLoop) -> None:
        self._topic = topic
        self._producer = producer
        self._consumer = consumer
        self._loop = loop

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            try:
                record = await self._consumer.get_one(self._topic)
                if record:
                    self._ctx_manager = record.i
                    return record
            except Exception as err:
                raise err

    async def commit(self):
        print(f'Committed offset {self._ctx_manager} in topic {self._topic}\n\n')

    async def publish(self, record: BaseRecord):
        await self._producer.send_and_wait(record, self._topic)

    # def __del__(self):
    #     asyncio.wait_for(self.stop(), loop=self._loop, timeout=5)


class StreamsManager:
    _streams_list: Dict[str, Stream]

    def __init__(self, topics: List[str], loop: AbstractEventLoop):
        self._producer = BaseProducer()
        self._consumer = BaseConsumer(topics)
        self._loop = loop

        self._streams_list = dict()

        for topic in topics:
            self._streams_list[topic] = Stream(topic, self._producer, self._consumer, self._loop)

    def __getitem__(self, topic):
        return self._streams_list[topic]

    def get_stream(self, topic):
        return self._streams_list[topic]

    def get_streams_list(self, topic):
        return self._streams_list

    async def start(self) -> None:
        await self._producer.start()
        await self._consumer.start()

    async def stop(self) -> None:
        await self._producer.stop()
        await self._consumer.stop()

    # def __del__(self):
    #     for topic, stream in self._streams_list.items():
    #         asyncio.wait_for(stream.stop(), loop=self._loop, timeout=5)


class Bartender:
    streams_manager: StreamsManager

    def __init__(self, streams_manager: StreamsManager) -> None:
        self.streams_manager = streams_manager

    async def consume_bills(self) -> None:
        async for bill in self.streams_manager['cash-register-events']:
            await self.handle_coffee_asked(bill)
            await self.streams_manager['cash-register-events'].commit()

    async def consume_coffee_machine_result(self) -> None:
        streams = self.streams_manager.get_streams_list()
        async for coffee_machine_res in streams['coffee-machine-results']:
            await self.handle_coffee_machine_result(coffee_machine_res)
            await self.streams_manager['coffee-machine-results'].commit()

    async def handle_coffee_asked(self, record: BaseRecord) -> None:
        # Do stuff
        print('Call handler handle_coffee_asked')
        coffee_cmd = BaseRecord('coffee-machine-commands', record.i)
        await self.streams_manager['coffee-machine-commands'].publish(coffee_cmd)

    async def handle_coffee_machine_result(self, result: BaseRecord) -> None:
        # Do stuff
        print('Call handler handle_coffee_machine_result')
        coffee_finished = BaseRecord('bartender-coffee-finished', result.i)
        await self.streams_manager['bartender-events'].publish(coffee_finished)


if __name__ == '__main__':
    rloop = asyncio.get_event_loop()

    bills_records: List[BaseRecord] = list()
    for i in range(0, 100):
        bills_records.append(BaseRecord('cash-register-events', i))
    coffee_machine_command: List[BaseRecord] = list()
    coffee_machine_result: List[BaseRecord] = list()
    bartender_event_records: List[BaseRecord] = list()

    m_streams = StreamsManager(['cash-register-events', 'bartender-events', 'coffee-machine-commands', 'coffee-machine-results'], rloop)

    bartender = Bartender(m_streams)

    asyncio.ensure_future(bartender.consume_bills(), loop=rloop)

    asyncio.ensure_future(bartender.consume_coffee_machine_result(), loop=rloop)

    rloop.run_forever()
