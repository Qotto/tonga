#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import asyncio
from logging import Logger
from aiokafka.producer import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.errors import KafkaError, KafkaTimeoutError

from typing import Union, List

from .base import BaseProducer
from ..serializer.base import BaseSerializer
from ...model.base import BaseModel
from ...model.exceptions.exceptions import KafkaProducerError, BadSerializer

__all__ = [
    'KafkaProducer',
]


class KafkaProducer(BaseProducer):
    logger: Logger
    serializer: BaseSerializer
    bootstrap_servers: str
    client_id: str
    acks: Union[int, str]
    running: bool
    kafka_producer: AIOKafkaProducer

    def __init__(self, bootstrap_servers: str, client_id: str, serializer: BaseSerializer,
                 acks: Union[int, str] = 1) -> None:
        super().__init__()
        self.logger = logging.getLogger(__name__)

        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.acks = acks
        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer('Bad serializer', 500)
        self.running = False

        try:
            loop = asyncio.get_event_loop()
            self.kafka_producer = AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap_servers,
                                                   client_id=self.client_id, acks=self.acks,
                                                   value_serializer=self.serializer.encode)
        except (ValueError, KafkaError) as err:
            raise KafkaProducerError(err.__str__(), 500)

    async def start_producer(self) -> None:
        try:
            await self.kafka_producer.start()
            self.running = True
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)

    async def stop_producer(self) -> None:
        try:
            await self.kafka_producer.stop()
            self.running = False
        except KafkaError as err:
            raise KafkaProducerError(err.__str__(), 500)

    async def send_and_await(self, event: BaseModel, topic: str) -> None:
        if not self.running:
            await self.start_producer()
        try:
            await self.kafka_producer.send_and_wait(topic=topic, value=event, key=event.partition_key)
        except (KafkaError, KafkaTimeoutError, KafkaProducerError) as err:
            raise KafkaProducerError(err.__str__(), 500)

    def create_batch(self) -> BatchBuilder:
        if not self.running:
            raise KafkaProducerError('Start producer before use', 500)
        return self.kafka_producer.create_batch()

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        if not self.running:
            raise KafkaProducerError('Start producer before use', 500)
        try:
            await self.kafka_producer.send_batch(batch=batch, topic=topic, partition=partition)
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)

    async def partitions_by_topic(self, topic: str) -> List[str]:
        if not self.running:
            raise KafkaProducerError('Start producer before use', 500)
        try:
            partitions = await self.kafka_producer.partitions_for(topic)
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)
        return partitions

    async def get_transactional_producer(self, transactional_uuid: str) -> AIOKafkaProducer:
        try:
            # Create Kafka transactional
            loop = asyncio.get_event_loop()
            transactional_kafka_producer = AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap_servers,
                                                            client_id=self.client_id, acks='all',
                                                            value_serializer=self.serializer.encode,
                                                            transactional_id=transactional_uuid)
        except (ValueError, KafkaError) as err:
            raise KafkaProducerError(err.__str__(), 500)
        return transactional_kafka_producer
