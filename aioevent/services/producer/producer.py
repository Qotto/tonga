#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
from asyncio import AbstractEventLoop
from logging import Logger
from aiokafka.producer import AIOKafkaProducer
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.errors import KafkaError, KafkaTimeoutError

from typing import Union, List

from aioevent.services.producer.base import BaseProducer
from aioevent.services.serializer.base import BaseSerializer
from aioevent.services.coordinator.partitioner.statefulset_partitioner import StatefulsetPartitioner
from aioevent.model.base import BaseModel
from aioevent.model.exceptions import KafkaProducerError, BadSerializer

__all__ = [
    'KafkaProducer',
]


class KafkaProducer(BaseProducer):
    """
    KafkaProducer Class, this class make bridge between AioKafkaProducer an AioEvent

    Attributes:
        name (str): Kafka Producer name
        logger (Logger): Python logger
        serializer (BaseSerializer): Serializer encode & decode event
        _bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                                    the consumer should contact to bootstrap initial cluster metadata
        _client_id (str): A name for this client. This string is passed in each request to servers and can be used
                          to identify specific server-side log entries that correspond to this client
        _acks (Union[int, str]): The number of acknowledgments the producer requires the leader to have
                                 received before considering a request complete. Possible value (0 / 1 / all)
        _running (bool): Is running flag
        _transactional_id (str): Id for make transactional process
        _kafka_producer (AIOKafkaProducer): AioKafkaProducer for more information go to
        _loop (AbstractEventLoop): Asyncio loop
    """
    name: str
    logger: Logger
    serializer: BaseSerializer
    _bootstrap_servers: Union[str, List[str]]
    _client_id: str
    _acks: Union[int, str]
    _running: bool
    _transactional_id: str
    _kafka_producer: AIOKafkaProducer
    _loop: AbstractEventLoop

    def __init__(self, name: str, bootstrap_servers: Union[str, List[str]], client_id: str, serializer: BaseSerializer,
                 loop: AbstractEventLoop, acks: Union[int, str] = 1, transactional_id: str = None) -> None:
        """
        KafkaProducer constructor

        Args:
            name (str): Kafka Producer name
            bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                                    the consumer should contact to bootstrap initial cluster metadata
            client_id (str): A name for this client. This string is passed in each request to servers and can be used
                            to identify specific server-side log entries that correspond to this client
            serializer (BaseSerializer): Serializer encode & decode event
            acks (Union[int, str]): The number of acknowledgments the producer requires the leader to have
                                 received before considering a request complete. Possible value (0 / 1 / all)
            transactional_id: Id for make transactional process

        Returns:
            None
        """
        super().__init__()
        self.name = name
        self.logger = logging.getLogger(__name__)

        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._acks = acks
        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer('Bad serializer', 500)
        self._transactional_id = transactional_id
        self._running = False
        self._loop = loop

        try:
            self._kafka_producer = AIOKafkaProducer(loop=self._loop, bootstrap_servers=self._bootstrap_servers,
                                                    client_id=self._client_id, acks=self._acks,
                                                    value_serializer=self.serializer.encode,
                                                    transactional_id=self._transactional_id,
                                                    partitioner=StatefulsetPartitioner())
        except (ValueError, KafkaError) as err:
            raise KafkaProducerError(err.__str__(), 500)
        self.logger.debug(f'Create new producer {client_id}')

    async def start_producer(self) -> None:
        """
        Start producer

        Returns:
            None
        """
        try:
            await self._kafka_producer.start()
            self._running = True
            self.logger.debug(f'Start producer : {self._client_id}')
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)

    async def stop_producer(self) -> None:
        """
        Stop producer

        Returns:
            None
        """
        try:
            await self._kafka_producer.stop()
            self._running = False
            self.logger.debug(f'Stop producer : {self._client_id}')
        except KafkaError as err:
            raise KafkaProducerError(err.__str__(), 500)

    async def send_and_await(self, event: BaseModel, topic: str) -> None:
        """
        This function send a massage and await an acknowledgments

        Args:
            event (BaseModel): Event to send in Kafka, inherit form BaseModel
            topic (str): Topic name to send massage

        Raises:
            ...
            # TODO write missing raises docstring

        Returns:
            None
        """
        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug(f'Send event {event.event_name()}')
            await self._kafka_producer.send_and_wait(topic=topic, value=event, key=event.partition_key)
        except (KafkaError, KafkaTimeoutError, KafkaProducerError) as err:
            self.logger.error(f'Kafka error = {err}')
            raise KafkaProducerError(err.__str__(), 500)
        except KeyError as er_key:
            self.logger.error(f'Key error = {er_key}')
            raise KafkaProducerError(f'KeyError, event not found in serializer, extra : {er_key}', 500)
        except Exception as er_ex:
            self.logger.error(f'Exception : {er_ex}')
            raise KafkaProducerError(er_ex.__str__(), 500)

    async def send(self, event: BaseModel, topic: str) -> None:
        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug(f'Send event {event.event_name()}')
            await self._kafka_producer.send(topic=topic, value=event, key=event.partition_key)
        except (KafkaError, KafkaTimeoutError, KafkaProducerError) as err:
            self.logger.error(f'Kafka error = {err}')
            raise KafkaProducerError(err.__str__(), 500)
        except KeyError as er_key:
            self.logger.error(f'Key error = {er_key}')
            raise KafkaProducerError(f'KeyError, event not found in serializer, extra : {er_key}', 500)
        except Exception as er_ex:
            self.logger.error(f'Exception : {er_ex}')
            raise KafkaProducerError(er_ex.__str__(), 500)

    async def create_batch(self) -> BatchBuilder:
        if not self._running:
            await self.start_producer()
        self.logger.debug(f'Create batch')
        return self._kafka_producer.create_batch()

    async def send_batch(self, batch: BatchBuilder, topic: str, partition: int = 0) -> None:
        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug(f'Send batch')
            await self._kafka_producer.send_batch(batch=batch, topic=topic, partition=partition)
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)

    async def partitions_by_topic(self, topic: str) -> List[str]:
        if not self._running:
            await self.start_producer()
        try:
            self.logger.debug(f'Get partitions by topic')
            partitions = await self._kafka_producer.partitions_for(topic)
        except (KafkaError, KafkaTimeoutError) as err:
            raise KafkaProducerError(err, 500)
        return partitions
