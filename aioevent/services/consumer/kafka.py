#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import asyncio
from logging import Logger

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka.errors import (
    TopicAuthorizationFailedError, OffsetOutOfRangeError,
    ConsumerStoppedError, NoOffsetForPartitionError, RecordTooLargeError)

from typing import List, Dict

from .base import BaseConsumer
from ..serializer.base import BaseSerializer
from ...app.base import BaseApp
from ...model.exceptions.exceptions import BadSerializer, KafkaConsumerError
from ...model.event.event import BaseEvent
from ...model.result.result import BaseResult
from ...model.command.command import BaseCommand

__all__ = [
    'KafkaConsumer',
]


class KafkaConsumer(BaseConsumer):
    logger: Logger
    serializer: BaseSerializer
    bootstrap_servers: str
    client_id: str
    topics: List[str]
    group_id: str
    auto_offset_reset: str
    max_retries: int
    retry_interval: int
    retry_backoff_coeff: int
    partition_key: bytes
    app: BaseApp
    isolation_level: str
    running: bool
    kafka_consumer = AIOKafkaConsumer

    current_offsets: Dict[TopicPartition, int]
    last_offsets: Dict[TopicPartition, int]
    last_committed_offsets: Dict[TopicPartition, int]

    def __init__(self, app: BaseApp, serializer: BaseSerializer, bootstrap_servers: str, client_id: str,
                 topics: List[str], group_id: str = None, partition_key: bytes = b'0',
                 auto_offset_reset: str = 'latest', max_retries: int = 10, retry_interval: int = 1000,
                 retry_backoff_coeff: int = 2, isolation_level: str = 'read_uncommitted') -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)

        self.app = app

        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer('Bad serializer', 500)

        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.topics = topics
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.retry_backoff_coeff = retry_backoff_coeff
        self.isolation_level = isolation_level
        self.partition_key = partition_key
        self.running = False

        self.current_offsets = dict()
        self.last_offsets = dict()
        self.last_committed_offsets = dict()

        try:
            loop = asyncio.get_event_loop()
            self.kafka_consumer = AIOKafkaConsumer(*self.topics, loop=loop, bootstrap_servers=self.bootstrap_servers,
                                                   client_id=client_id, group_id=group_id,
                                                   value_deserializer=self.serializer.decode,
                                                   auto_offset_reset=self.auto_offset_reset,
                                                   isolation_level=self.isolation_level, enable_auto_commit=False)
        except (ValueError, KafkaError) as err:
            raise KafkaConsumerError(err.__str__(), 500)

    async def start_consumer(self) -> None:
        try:
            await self.kafka_consumer.start()
            self.running = True
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaConsumerError(err.__str__(), 500)

    async def stop_consumer(self) -> None:
        try:
            await self.kafka_consumer.stop()
            self.running = False
        except KafkaError as err:
            raise KafkaConsumerError(err.__str__(), 500)

    async def get_last_committed_offsets(self) -> Dict[TopicPartition, int]:
        offsets: Dict[TopicPartition, int] = dict()
        for partition in self.kafka_consumer.assignment():
            offset = await self.kafka_consumer.committed(partition)
            offsets[partition] = offset
        return offsets

    async def get_current_offsets(self) -> Dict[TopicPartition, int]:
        offsets: Dict[TopicPartition, int] = dict()
        for partition in self.kafka_consumer.assignment():
            offset = await self.kafka_consumer.position(partition)
            offsets[partition] = offset
        return offsets

    async def get_beginning_offsets(self) -> Dict[TopicPartition, int]:
        offsets: Dict[TopicPartition, int] = dict()
        for partition in self.kafka_consumer.assignment():
            offset = (await self.kafka_consumer.beginning_offsets([partition]))[partition]
            offsets[partition] = offset
        return offsets

    async def get_last_offsets(self) -> Dict[TopicPartition, int]:
        offsets: Dict[TopicPartition, int] = dict()
        for partition in self.kafka_consumer.assignment():
            offset = (await self.kafka_consumer.end_offsets([partition]))[partition]
            offsets[partition] = offset
        return offsets

    async def load_offsets(self, mod: str = 'earliest') -> None:
        if not self.running:
            await self.start_consumer()
        if mod == 'latest':
            await self.seek_to_end()
        elif mod == 'earliest':
            await self.seek_to_beginning()
        elif mod == 'committed':
            await self.seek_to_last_commit()
            print('seek committed')
        else:
            raise KafkaConsumerError('Unknown mod', 500)

        self.current_offsets = await self.get_current_offsets()
        self.last_offsets = await self.get_last_offsets()

        if self.group_id is not None:
            self.last_committed_offsets = await self.get_last_committed_offsets()
            for tp, offset in self.last_committed_offsets.items():
                if offset is None:
                    print('seek earliest')
                    await self.seek_to_beginning()
                    break

    async def listen_event(self, mod: str = 'earliest') -> None:
        if not self.running:
            await self.load_offsets(mod)

        self.pprint_consumer_offsets()
        async for msg in self.kafka_consumer:
            self.logger.debug(f'New Message on consumer {self.client_id}, topic : {msg.topic}, '
                              f'partition : {msg.partition}')
            print(f'New Message on consumer {self.client_id}, topic : {msg.topic}, partition : {msg.partition}')
            self.pprint_consumer_offsets()

            tp = TopicPartition(msg.topic, msg.partition)
            sleep_duration_in_ms = self.retry_interval
            for retries in range(0, self.max_retries):
                try:
                    event = msg.value
                    result = None
                    if isinstance(event, BaseEvent):
                        await event.handle(app=self.app, corr_id=event.correlation_id, group_id=self.group_id,
                                           topic=tp, offset=msg.offset)
                    elif isinstance(event, BaseCommand):
                        result = await event.execute(app=self.app, corr_id=event.correlation_id, group_id=self.group_id,
                                                     topic=tp, offset=msg.offset)
                    elif isinstance(event, BaseResult):
                        result = await event.on_result(app=self.app, corr_id=event.correlation_id,
                                                       group_id=self.group_id, topic=tp, offset=msg.offset)
                    else:
                        raise ValueError

                    if result is None:
                        self.current_offsets[tp] = msg.offset + 1
                        if self.group_id is not None:
                            if self.last_committed_offsets[tp] is None or \
                                    self.last_committed_offsets[tp] < self.current_offsets[tp]:
                                await self.kafka_consumer.commit(self.current_offsets)
                                self.last_committed_offsets[tp] = msg.offset + 1
                    elif result == 'transaction':
                        self.last_committed_offsets = await self.get_last_committed_offsets()
                    break
                except Exception as e:
                    print('Listen Event exception : ', e)
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self.retry_backoff_coeff
                    if retries not in range(0, self.max_retries):
                        await self.stop_consumer()
                        print('CRITICAL FAILURE')
                        exit(1)

    def is_ready(self) -> bool:
        if self.last_offsets == self.current_offsets:
            return True
        return False

    async def listen_building_state(self) -> None:
        await self.load_offsets('earliest')
        self.pprint_consumer_offsets()

        if self.is_ready():
            await self.stop_consumer()
            return None

        async for msg in self.kafka_consumer:
            self.logger.debug(f'New Message on consumer {self.client_id}, topic : {msg.topic}, '
                              f'partition : {msg.partition}')
            self.pprint_consumer_offsets()

            tp = TopicPartition(msg.topic, msg.partition)
            sleep_duration_in_ms = self.retry_interval
            for retries in range(0, self.max_retries):
                try:
                    event = msg.value

                    if isinstance(event, BaseEvent):
                        print(f'Event name = {event.event_name()}')
                        await event.handle(app=self.app, corr_id=event.correlation_id, group_id=self.group_id,
                                           topic=tp, offset=msg.offset)
                    elif isinstance(event, BaseCommand):
                        print(f'Command name = {event.event_name()}')
                        await event.execute(app=self.app, corr_id=event.correlation_id, group_id=self.group_id,
                                            topic=tp, offset=msg.offset)
                    elif isinstance(event, BaseResult):
                        print(f'Result name = {event.event_name()}')
                        await event.on_result(app=self.app, corr_id=event.correlation_id,
                                              group_id=self.group_id, topic=msg.tp, offset=msg.offset)
                    else:
                        raise ValueError
                    self.current_offsets[tp] = msg.offset + 1
                    break
                except Exception as e:
                    print(e)
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self.retry_backoff_coeff
                    if retries not in range(0, self.max_retries):
                        await self.stop_consumer()
                        print('CRITICAL FAILURE')
                        exit(1)
            if self.is_ready():
                await self.stop_consumer()
                return None

    async def get_next(self):
        while True:
            try:
                yield self.kafka_consumer.getone()
            except ConsumerStoppedError:
                raise StopAsyncIteration
            except (TopicAuthorizationFailedError,
                    OffsetOutOfRangeError,
                    NoOffsetForPartitionError) as err:
                raise KafkaConsumerError(err, 500)
            except RecordTooLargeError:
                pass

    async def get_many(self, partitions: List[int] = None, max_records: int = None):
        if not self.running:
            await self.start_consumer()
        return await self.kafka_consumer.getmany(*partitions, max_records=max_records)

    async def get_one(self, partitions: List[int] = None):
        if not self.running:
            await self.start_consumer()
        return await self.kafka_consumer.getone(*partitions)

    async def seek_to_beginning(self, partition: int = None, topic: str = None) -> None:
        if not self.running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            await self.kafka_consumer.seek_to_beginning(TopicPartition(topic, partition))
        else:
            await self.kafka_consumer.seek_to_beginning()

    async def seek_to_end(self, partition: int = None, topic: str = None) -> None:
        if not self.running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            await self.kafka_consumer.seek_to_end(TopicPartition(topic, partition))
        else:
            await self.kafka_consumer.seek_to_end()

    async def seek_to_last_commit(self, partition: int = None, topic: str = None) -> None:
        if not self.running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            await self.kafka_consumer.seek_to_committed(TopicPartition(topic, partition))
        else:
            await self.kafka_consumer.seek_to_committed()

    async def seek_custom(self, partition: int = None, topic: str = None,  offset: int = None) -> None:
        if not self.running:
            await self.start_consumer()
        if partition is not None and topic is not None and offset is not None:
            await self.kafka_consumer.seek(TopicPartition(topic, partition), offset)
        else:
            raise KafkaConsumerError('Custom seek need 3 argv', 500)

    async def subscriptions(self) -> frozenset:
        if not self.running:
            await self.start_consumer()
        return self.kafka_consumer.subscription()

    def pprint_consumer_offsets(self):
        self.logger.debug(f'Client ID = {self.client_id}')
        print(f'Client ID = {self.client_id}')

        self.logger.debug(f'Current Offset =  {self.current_offsets}')
        self.logger.debug(f'Last Offset = {self.last_offsets}')
        print(f'Current Offset =  {self.current_offsets}')
        print(f'Last Offset = {self.last_offsets}')

        self.logger.debug(f'Last committed offset = {self.last_committed_offsets}')
        print(f'Last committed offset = {self.last_committed_offsets}')
