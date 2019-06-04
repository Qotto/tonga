#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import json
import copy
import asyncio
from asyncio import AbstractEventLoop
from logging import Logger

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka import TopicPartition
from aiokafka.errors import KafkaError, KafkaTimeoutError
from aiokafka.errors import (TopicAuthorizationFailedError, OffsetOutOfRangeError, IllegalStateError,
                             UnsupportedVersionError, ConsumerStoppedError, NoOffsetForPartitionError,
                             RecordTooLargeError, CommitFailedError, ConnectionError)

from typing import List, Dict, Any, Union

# Model import
from tonga.models.events.base import BaseModel
from tonga.models.handlers.base import BaseHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
from tonga.models.handlers.result.result_handler import BaseResultHandler
from tonga.models.handlers.command.command_handler import BaseCommandHandler

from tonga.models.store_record.base import BaseStoreRecord, BaseStoreRecordHandler

# BaseConsumer import
from tonga.services.consumer.base import BaseConsumer

# BaseSerializer / KafkaKeySerializer import
from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.kafka_key import KafkaKeySerializer

# Import StatefulsetPartitionAssignor
from tonga.services.coordinator.assignors.statefulset_assignors import StatefulsetPartitionAssignor

# BaseStoreBuilder import
from tonga.stores.store_builder.base import BaseStoreBuilder

# Exception import
from tonga.services.errors import BadSerializer
from tonga.services.consumer.errors import (ConsumerConnectionError, AioKafkaConsumerBadParams,
                                               KafkaConsumerError, ConsumerKafkaTimeoutError,
                                               IllegalOperation, TopicPartitionError,
                                               NoPartitionAssigned, OffsetError, UnknownStoreRecordHandler,
                                               UnknownHandler, UnknownHandlerReturn,
                                               HandlerException, KafkaConsumerAlreadyStartedError,
                                               KafkaConsumerNotStartedError)

__all__ = [
    'KafkaConsumer',
]


class KafkaConsumer(BaseConsumer):
    """
    KafkaConsumer Class

    Attributes:
        serializer (BaseSerializer): Serializer encode & decode event
        _bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                 the consumer should contact to bootstrap initial cluster metadata
        _client_id (str): A name for this client. This string is passed in each request to servers and can be used
                          to identify specific server-side log entries that correspond to this client
        _topics (List[str]): List of topics to subscribe to
        _group_id (str): name of the consumer group, and to use for fetching and committing offsets.
                        If None, offset commits are disabled
        _auto_offset_reset (str): A policy for resetting offsets on OffsetOutOfRange errors: ‘earliest’ will move to
                                  the oldest available message, ‘latest’ will move to the most recent.
                                  Any other value will raise the exception
        _max_retries (int): Number of retries before critical failure
        _retry_interval (int): Interval before next retries
        _retry_backoff_coeff (int): Backoff coeff for next retries
        _isolation_level (str): Controls how to read messages written transactionally. If set to read_committed,
                                will only return transactional messages which have been committed.
                                If set to read_uncommitted, will return all messages, even transactional messages
                                which have been aborted. Non-transactional messages will be returned unconditionally in
                                either mode.
        _assignors_data (Dict[str, Any]): Dict with assignors information, more details in StatefulsetPartitionAssignor
        _store_builder (BaseStoreBuilder): If this flag is step consumer run build_stores() otherwise listen_event
                                           was started
        _running (bool): Is running flag
        _kafka_consumer (AIOKafkaConsumer): AioKafkaConsumer for more information go to
        __current_offsets (Dict[TopicPartition, int]): Contains current TopicPartition and offsets
        __last_offsets (Dict[TopicPartition, int]): Contains last TopicPartition and offsets
        __last_committed_offsets (Dict[TopicPartition, int]): Contains last committed TopicPartition and offsets
        _loop (AbstractEventLoop): Asyncio loop
        logger (Logger): Python logger
    """
    serializer: BaseSerializer
    _bootstrap_servers: Union[str, List[str]]
    _client_id: str
    _topics: List[str]
    _group_id: str
    _auto_offset_reset: str
    _max_retries: int
    _retry_interval: int
    _retry_backoff_coeff: int
    _isolation_level: str
    _assignors_data: Dict[str, Any]
    _store_builder: BaseStoreBuilder
    _running: bool
    _kafka_consumer: AIOKafkaConsumer

    __current_offsets: Dict[TopicPartition, int]
    __last_offsets: Dict[TopicPartition, int]
    __last_committed_offsets: Dict[TopicPartition, int]

    _loop: AbstractEventLoop
    logger: Logger

    def __init__(self, name: str, serializer: BaseSerializer, bootstrap_servers: Union[str, List[str]],
                 client_id: str, topics: List[str], loop: AbstractEventLoop,
                 group_id: str = None, auto_offset_reset: str = 'earliest', max_retries: int = 10,
                 retry_interval: int = 1000, retry_backoff_coeff: int = 2, assignors_data=None,
                 store_builder: BaseStoreBuilder = None, isolation_level: str = 'read_uncommitted') -> None:
        """
        KafkaConsumer constructor

        Args:
            name (str): Kafka consumer name
            serializer (BaseSerializer): Serializer encode & decode event
            bootstrap_servers (Union[str, List[str]): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that
                                                       the consumer should contact to bootstrap initial cluster metadata
            client_id (str): A name for this client. This string is passed in each request to servers and can be used
                            to identify specific server-side log entries that correspond to this client
            topics (List[str]): List of topics to subscribe to
            loop (AbstractEventLoop): Asyncio loop
            group_id (str): name of the consumer group, and to use for fetching and committing offsets.
                            If None, offset commits are disabled
            auto_offset_reset (str): A policy for resetting offsets on OffsetOutOfRange errors: ‘earliest’ will move to
                                     the oldest available message, ‘latest’ will move to the most recent.
                                     Any other value will raise the exception
            max_retries (int): Number of retries before critical failure
            retry_interval (int): Interval before next retries
            retry_backoff_coeff (int): Backoff coeff for next retries
            assignors_data (Dict[str, Any]): Dict with assignors information, more details in
                                             StatefulsetPartitionAssignor
            store_builder (bool): If this flag is step consumer run build_stores() otherwise listen_event was started
            isolation_level (str): Controls how to read messages written transactionally. If set to read_committed,
                                   will only return transactional messages which have been committed.
                                   If set to read_uncommitted, will return all messages, even transactional messages
                                   which have been aborted. Non-transactional messages will be returned unconditionally
                                   in either mode.

        Returns:
            None
        """
        super().__init__()
        self.name = name

        if assignors_data is None:
            assignors_data = {}

        self.logger = logging.getLogger('tonga')

        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer

        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._topics = topics
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._max_retries = max_retries
        self._retry_interval = retry_interval
        self._retry_backoff_coeff = retry_backoff_coeff
        self._isolation_level = isolation_level
        self._assignors_data = assignors_data
        self._store_builder = store_builder
        self._running = False
        self._loop = loop

        self.__current_offsets = dict()
        self.__last_offsets = dict()
        self.__last_committed_offsets = dict()

        try:
            self.logger.info(json.dumps(assignors_data))
            statefulset_assignor = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data), 'utf-8'))
            self._kafka_consumer = AIOKafkaConsumer(*self._topics, loop=self._loop,
                                                    bootstrap_servers=self._bootstrap_servers,
                                                    client_id=client_id, group_id=group_id,
                                                    value_deserializer=self.serializer.decode,
                                                    auto_offset_reset=self._auto_offset_reset,
                                                    isolation_level=self._isolation_level, enable_auto_commit=False,
                                                    key_deserializer=KafkaKeySerializer.decode,
                                                    partition_assignment_strategy=[statefulset_assignor])
        except KafkaError as err:
            self.logger.exception(f'{err.__str__()}')
            raise err
        except ValueError as err:
            self.logger.exception(f'{err.__str__()}')
            raise AioKafkaConsumerBadParams
        self.logger.debug(f'Create new consumer {client_id}, group_id {group_id}')

    async def start_consumer(self) -> None:
        """
        Start consumer

        Returns:
            None

        Raises:
            AttributeError: KafkaConsumerError
            ValueError: If KafkaError or KafkaTimoutError is raised, exception value is contain
                        in KafkaConsumerError.msg
        """
        if self._running:
            raise KafkaConsumerAlreadyStartedError
        for retry in range(2):
            try:
                await self._kafka_consumer.start()
                self._running = True
                self.logger.debug(f'Start consumer : {self._client_id}, group_id : {self._group_id}')
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except ConnectionError as err:
                self.logger.exception(f'{err.__str__()}')
                await asyncio.sleep(1)
            except KafkaError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            else:
                break
        else:
            raise ConsumerConnectionError

    async def stop_consumer(self) -> None:
        """
        Stop consumer

        Returns:
            None

        Raises:
            AttributeError: KafkaConsumerError
            ValueError: If KafkaError is raised, exception value is contain
                        in KafkaConsumerError.msg
        """
        if not self._running:
            raise KafkaConsumerNotStartedError
        try:
            await self._kafka_consumer.stop()
            self._running = False
            self.logger.debug(f'Stop consumer : {self._client_id}, group_id : {self._group_id}')
        except KafkaTimeoutError as err:
            self.logger.exception(f'{err.__str__()}')
            raise ConsumerKafkaTimeoutError
        except KafkaError as err:
            self.logger.exception(f'{err.__str__()}')
            raise err

    def is_running(self) -> bool:
        return self._running

    async def get_last_committed_offsets(self) -> Dict[TopicPartition, int]:
        """
        Get last committed offsets

        Returns:
            Dict[TopicPartition, int]: Contains all assigned partitions with last committed offsets
        """
        last_committed_offsets: Dict[TopicPartition, int] = dict()
        self.logger.debug(f'Get last committed offsets')
        if self._group_id is None:
            raise IllegalOperation
        for partition in self._kafka_consumer.assignment():
            offset = await self._kafka_consumer.committed(partition)
            last_committed_offsets[partition] = offset
        return last_committed_offsets

    async def get_current_offsets(self) -> Dict[TopicPartition, int]:
        """
        Get current offsets

        Returns:
            Dict[TopicPartition, int]: Contains all assigned partitions with current offsets
        """
        current_offsets: Dict[TopicPartition, int] = dict()
        self.logger.debug(f'Get current offsets')
        for partition in self._kafka_consumer.assignment():
            try:
                offset = await self._kafka_consumer.position(partition)
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            current_offsets[partition] = offset
        return current_offsets

    async def get_beginning_offsets(self) -> Dict[TopicPartition, int]:
        """
        Get beginning offsets

        Returns:
            Dict[TopicPartition, int]: Contains all assigned partitions with beginning offsets
        """
        beginning_offsets: Dict[TopicPartition, int] = dict()
        self.logger.debug(f'Get beginning offsets')
        for partition in self._kafka_consumer.assignment():
            try:
                offset = (await self._kafka_consumer.beginning_offsets([partition]))[partition]
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                raise ConsumerKafkaTimeoutError
            except UnsupportedVersionError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            beginning_offsets[partition] = offset
        return beginning_offsets

    async def get_last_offsets(self) -> Dict[TopicPartition, int]:
        """
        Get last offsets

        Returns:
            Dict[TopicPartition, int]: Contains all assigned partitions with last offsets
        """
        last_offsets: Dict[TopicPartition, int] = dict()
        self.logger.debug(f'Get last offsets')
        for partition in self._kafka_consumer.assignment():
            try:
                offset = (await self._kafka_consumer.end_offsets([partition]))[partition]
            except KafkaTimeoutError as err:
                self.logger.exception(f'{err.__str__()}')
                raise ConsumerKafkaTimeoutError
            except UnsupportedVersionError as err:
                self.logger.exception(f'{err.__str__()}')
                raise err
            last_offsets[partition] = offset
        return last_offsets

    async def load_offsets(self, mod: str = 'earliest') -> None:
        """
        This method was call before consume topics, assign position to consumer

        Args:
            mod: Start position of consumer (earliest, latest, committed)

        Returns:
            None
        """
        self.logger.debug(f'Load offset mod : {mod}')
        if not self._running:
            await self.start_consumer()

        if mod == 'latest':
            await self.seek_to_end()
        elif mod == 'earliest':
            await self.seek_to_beginning()
        elif mod == 'committed':
            await self.seek_to_last_commit()
        else:
            raise KafkaConsumerError('Unknown mod', 500)

        self.__current_offsets = await self.get_current_offsets()
        self.__last_offsets = await self.get_last_offsets()

        if self._group_id is not None:
            self.__last_committed_offsets = await self.get_last_committed_offsets()
            for tp, offset in self.__last_committed_offsets.items():
                if offset is None:
                    self.logger.debug(f'Seek to beginning, no committed offsets was found')
                    await self.seek_to_beginning(tp.partition, tp.topic)

    async def debug_print_all_msg(self):
        """
        Debug method, useful for display all msg was contained in assigned topic/partitions

        Returns:
            None
        """
        while True:
            message = await self._kafka_consumer.getone()
            self.logger.info('----------------------------------------------------------------------------------------')
            self.logger.info(f'Topic {message.topic}, Partition {message.partition}, Offset {message.offset}, '
                             f'Key {message.key}, Value {message.value}, Headers {message.headers}')
            self.logger.info('----------------------------------------------------------------------------------------')

    async def listen_event(self, mod: str = 'earliest') -> None:
        """
        Listens events from assigned topic / partitions

        Args:
            mod: Start position of consumer (earliest, latest, committed)

        Returns:
            None
        """
        if not self._running:
            await self.load_offsets(mod)

        self.pprint_consumer_offsets()

        # await self.getone()

        async for msg in self._kafka_consumer:
            # Debug Display
            self.logger.debug("---------------------------------------------------------------------------------")
            self.logger.debug(f'New Message on consumer {self._client_id}, Topic {msg.topic}, '
                              f'Partition {msg.partition}, Offset {msg.offset}, Key {msg.key}, Value {msg.value},'
                              f'Headers {msg.headers}')
            self.pprint_consumer_offsets()
            self.logger.debug("---------------------------------------------------------------------------------")

            tp = TopicPartition(msg.topic, msg.partition)
            self.__current_offsets[tp] = msg.offset
            # self.last_offsets = await self.get_last_offsets()

            sleep_duration_in_ms = self._retry_interval
            for retries in range(0, self._max_retries):
                try:
                    decode_dict = msg.value
                    event_class = decode_dict['event_class']
                    handler_class = decode_dict['handler_class']

                    logging.debug(f'Event name : {event_class.event_name()}  Event content :\n{event_class.__dict__}')

                    # Calls handle if event is instance BaseHandler
                    if isinstance(handler_class, BaseEventHandler):
                        result = await handler_class.handle(event=event_class, group_id=self._group_id, tp=tp,
                                                            offset=msg.offset)
                    elif isinstance(handler_class, BaseCommandHandler):
                        result = await handler_class.execute(event=event_class, group_id=self._group_id, tp=tp,
                                                             offset=msg.offset)
                    elif isinstance(handler_class, BaseResultHandler):
                        result = await handler_class.on_result(event=event_class, group_id=self._group_id, tp=tp,
                                                               offset=msg.offset)
                    else:
                        # Otherwise raise KafkaConsumerUnknownHandler
                        raise UnknownHandler

                    # If result is none (no transactional process), check if consumer has an
                    # group_id (mandatory to commit in Kafka)
                    if result is None and self._group_id is not None:
                        # Check if next commit was possible (Kafka offset)
                        if self.__last_committed_offsets[tp] is None or \
                                self.__last_committed_offsets[tp] <= self.__current_offsets[tp]:
                            self.logger.debug(f'Commit msg {event_class.event_name()} in topic {msg.topic} partition '
                                              f'{msg.partition} offset {self.__current_offsets[tp] + 1}')
                            await self._kafka_consumer.commit({tp: self.__current_offsets[tp] + 1})
                            self.__last_committed_offsets[tp] = msg.offset + 1

                    # Transactional process no commit
                    elif result == 'transaction':
                        self.logger.debug(f'Transaction end')
                        self.__current_offsets = await self.get_current_offsets()
                        self.__last_committed_offsets = await self.get_last_committed_offsets()
                    # Otherwise raise KafkaConsumerUnknownHandlerReturn
                    else:
                        raise UnknownHandlerReturn

                    # Break if everything was successfully processed
                    break
                except IllegalStateError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise NoPartitionAssigned
                except ValueError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise OffsetError
                except CommitFailedError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise err
                except (KafkaError, HandlerException) as err:
                    self.logger.exception(f'{err.__str__()}')
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self._retry_backoff_coeff
                    if retries not in range(0, self._max_retries):
                        await self.stop_consumer()
                        logging.error(f'Max retries, close consumer and exit')
                        exit(1)

    def check_if_store_is_ready(self) -> None:
        # TODO Optimize check store is ready
        if self._store_builder.get_local_store().is_initialized() and self._store_builder.get_global_store().is_initialized():
            return

        self.logger.info('-------------------------------------------------------')
        ack = False
        for tp, offset in self.__last_offsets.items():
            self.logger.info(f'Topic : {tp.topic}, partition : {tp.partition}, offset: {offset}')
            if tp.partition == self._store_builder.get_current_instance():
                self.logger.info('Own partition')
                if offset == 0:
                    self._store_builder.get_local_store().set_initialized(True)
                else:
                    ack = False
            else:
                ack = True if offset == 0 else False
        if ack:
            self._store_builder.get_global_store().set_initialized(True)
        self.logger.info('-------------------------------------------------------')

    async def listen_store_records(self, rebuild: bool = False) -> None:
        """
        Listens events for store construction

        Args:
            rebuild (bool): if true consumer seek to fist offset for rebuild own state

        Returns:
            None
        """
        if self._store_builder is None:
            raise KeyError

        self.logger.info('Start listen store records')

        await self.start_consumer()
        await self._store_builder.initialize_store_builder()

        if not self._running:
            raise KafkaConsumerError('Fail to start tongaConsumer', 500)

        # Check if store is ready
        self.check_if_store_is_ready()
        self.pprint_consumer_offsets()

        async for msg in self._kafka_consumer:
            # Debug Display
            self.logger.debug("---------------------------------------------------------------------")
            self.logger.debug(f'New Message on store builder consumer {self._client_id}, Topic {msg.topic}, '
                              f'Partition {msg.partition}, Offset {msg.offset}, Key {msg.key}, Value {msg.value},'
                              f'Headers {msg.headers}')
            self.pprint_consumer_offsets()
            self.logger.debug("---------------------------------------------------------------------")

            # Check if store is ready
            self.check_if_store_is_ready()

            tp = TopicPartition(msg.topic, msg.partition)
            self.__current_offsets[tp] = msg.offset

            sleep_duration_in_ms = self._retry_interval
            for retries in range(0, self._max_retries):
                try:
                    decode_dict = msg.value
                    event_class: BaseModel = decode_dict['event_class']
                    handler_class: BaseStoreRecordHandler = decode_dict['handler_class']

                    logging.debug(f'Store event name : {event_class.event_name()}\nEvent '
                                  f'content :\n{event_class.__dict__}\n')

                    result = None
                    if msg.partition == self._store_builder.get_current_instance():
                        # Calls local_state_handler if event is instance BaseStorageBuilder
                        if rebuild and not self._store_builder.get_local_store().is_initialized():
                            if isinstance(event_class, BaseStoreRecord):
                                result = await handler_class.local_store_handler(store_record=event_class,
                                                                                 group_id=self._group_id, tp=tp,
                                                                                 offset=msg.offset)
                            else:
                                raise UnknownStoreRecordHandler
                    elif msg.partition != self._store_builder.get_current_instance():
                        if isinstance(event_class, BaseStoreRecord):
                            result = await handler_class.global_store_handler(store_record=event_class,
                                                                              group_id=self._group_id, tp=tp,
                                                                              offset=msg.offset)
                        else:
                            raise UnknownStoreRecordHandler

                    # If result is none (no transactional process), check if consumer has an
                    # group_id (mandatory to commit in Kafka)
                    # TODO Add commit store later V2
                    # if result is None and self._group_id is not None:
                    #     # Check if next commit was possible (Kafka offset)
                    #     if self.__last_committed_offsets[tp] is None or \
                    #             self.__last_committed_offsets[tp] <= self.__current_offsets[tp]:
                    #         self.logger.debug(f'Commit msg {event_class.event_name()} in topic {msg.topic} partition '
                    #                           f'{msg.partition} offset {self.__current_offsets[tp] + 1}')
                    #         await self._kafka_consumer.commit({tp: self.__current_offsets[tp] + 1})
                    #         self.__last_committed_offsets[tp] = msg.offset + 1
                    # # Otherwise raise ValueError
                    # else:
                    #     raise ValueError

                    # Break if everything was successfully processed
                    break
                except IllegalStateError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise NoPartitionAssigned
                except ValueError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise OffsetError
                except CommitFailedError as err:
                    self.logger.exception(f'{err.__str__()}')
                    raise err
                except (KafkaError, HandlerException) as err:
                    self.logger.exception(f'{err.__str__()}')
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self._retry_backoff_coeff
                    if retries not in range(0, self._max_retries):
                        await self.stop_consumer()
                        logging.error(f'Max retries, close consumer and exit')
                        exit(1)

    def is_lag(self) -> bool:
        """
        Consumer has lag ?

        Returns:
            bool: True if consumer is lagging otherwise return false and consumer is up to date
        """
        if self.__last_offsets == self.__current_offsets:
            return False
        return True

    async def get_next(self):
        """
        Async generator for consumer msg

        Yields:
            Any: yields new msg
        """
        while True:
            try:
                yield self._kafka_consumer.getone()
            except ConsumerStoppedError:
                raise StopAsyncIteration
            except (TopicAuthorizationFailedError,
                    OffsetOutOfRangeError,
                    NoOffsetForPartitionError) as err:
                raise KafkaConsumerError(err, 500)
            except RecordTooLargeError:
                pass

    async def get_many(self, partitions: List[TopicPartition] = None, max_records: int = None) -> Dict[Any, Any]:
        """
        Get many msg in batch

        Args:
            partitions (List[TopicPartition]): List of topic / partition to get many msg
            max_records (int): Max msg records

        Returns:
            Dict[Any, Any]: Return a dict contain msg
        """
        if not self._running:
            await self.start_consumer()
        self.logger.debug(f'Get many in partitions : {partitions}')
        return await self._kafka_consumer.getmany(*partitions, max_records=max_records)

    async def get_one(self, partitions: List[TopicPartition] = None) -> Any:
        """
        Get one message

        Args:
            partitions (List[TopicPartition]): List of topic / partition to get many msg

        Returns:
            Any: return msg
        """
        if not self._running:
            await self.start_consumer()
        self.logger.debug(f'Get one in partitions : {partitions}')
        return await self._kafka_consumer.getone(*partitions)

    async def seek_to_beginning(self, partition: int = None, topic: str = None) -> None:
        """
        Seek to fist offset, mod 'earliest'

        Args:
            partition (int): Partition value
            topic (str): Topic name

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            try:
                await self._kafka_consumer.seek_to_beginning(TopicPartition(topic, partition))
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TopicPartitionError
            self.logger.debug(f'Seek to beginning for topic : {topic}, partition : {partition}')
        else:
            try:
                await self._kafka_consumer.seek_to_beginning()
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            self.logger.debug(f'Seek to beginning for all topics & partitions')

    async def seek_to_end(self, partition: int = None, topic: str = None) -> None:
        """
        Seek to latest offset, mod 'latest'

        Args:
            partition (int): Partition value
            topic (str): Topic name

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            try:
                await self._kafka_consumer.seek_to_end(TopicPartition(topic, partition))
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TopicPartitionError
            self.logger.debug(f'Seek to end for topic : {topic}, partition : {partition}')
        else:
            try:
                await self._kafka_consumer.seek_to_end()
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            self.logger.debug(f'Seek to end for all topics & partitions')

    async def seek_to_last_commit(self, partition: int = None, topic: str = None) -> None:
        """
        Seek to last committed offsets, mod 'committed'

        Args:
            partition (int): Partition value
            topic (str): Topic name

        Returns:
            None
        """
        if self._group_id is None:
            raise IllegalOperation
        if not self._running:
            await self.start_consumer()
        if partition is not None and topic is not None:
            try:
                await self._kafka_consumer.seek_to_committed(TopicPartition(topic, partition))
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TopicPartitionError
            self.logger.debug(f'Seek to last committed for topic : {topic}, partition : {partition}')
        else:
            try:
                await self._kafka_consumer.seek_to_committed()
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            self.logger.debug(f'Seek to last committed for all topics & partitions')

    async def seek_custom(self, topic: str = None,  partition: int = None, offset: int = None) -> None:
        """
        Seek to custom offsets

        Args:
            partition (int): Partition value
            topic (str): Topic name
            offset (int): Offset value

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if partition is not None and topic is not None and offset is not None:
            try:
                await self._kafka_consumer.seek(TopicPartition(topic, partition), offset)
            except ValueError as err:
                self.logger.exception(f'{err.__str__()}')
                raise OffsetError
            except TypeError as err:
                self.logger.exception(f'{err.__str__()}')
                raise TopicPartitionError
            except IllegalStateError as err:
                self.logger.exception(f'{err.__str__()}')
                raise NoPartitionAssigned
            self.logger.debug(f'Custom seek for topic : {topic}, partition : {partition}, offset : {offset}')
        else:
            raise KafkaConsumerError('Custom seek need 3 argv', 500)

    async def subscriptions(self) -> frozenset:
        """
        Get list of subscribed topic

        Returns:
            frozenset: List of subscribed topic

        """
        if not self._running:
            await self.start_consumer()
        return self._kafka_consumer.subscription()

    def pprint_consumer_offsets(self) -> None:
        """
        Debug tool, print all consumer position

        Returns:
            None
        """
        self.logger.debug(f'Client ID = {self._client_id}')

        self.logger.debug(f'Current Offset =  {self.__current_offsets}')
        self.logger.debug(f'Last Offset = {self.__last_offsets}')

        self.logger.debug(f'Last committed offset = {self.__last_committed_offsets}')

    def get_aiokafka_consumer(self) -> AIOKafkaConsumer:
        """
        Get aiokafka consumer

        Returns:
            AIOKafkaConsumer: Current instance of AIOKafkaConsumer
        """
        return self._kafka_consumer

    def get_offset_bundle(self) -> Dict[str, Dict[TopicPartition, int]]:
        """
        Return a bundle with each assigned assigned topic/partition with current, latest, last committed
        topic/partition as dict

        Returns:
            Dict[str, Dict[TopicPartition, int]]: Contains current_offset / last_offset / last_committed_offset
        """
        return {
            'current_offset': self.__current_offsets.copy(),
            'last_offset': self.__last_offsets.copy(),
            'last_committed_offset': self.__last_committed_offsets.copy()
        }

    def get_current_offset(self) -> Dict[TopicPartition, int]:
        """
        Return current offset of each assigned topic/partition

        Returns:
            Dict[TopicPartition, int]: Dict contains current offset of each assigned partition
        """
        return self.__current_offsets.copy()

    def get_last_offset(self) -> Dict[TopicPartition, int]:
        """
        Return last offset of each assigned topic/partition

        Returns:
            Dict[TopicPartition, int]: Dict contains latest offset of each assigned partition
        """
        return self.__last_offsets.copy()

    def get_last_committed_offset(self) -> Dict[TopicPartition, int]:
        """
        Return last committed offset of each assigned topic/partition

        Returns:
            Dict[TopicPartition, int]: Dict contains last committed offset of each assigned partition
        """
        return self.__last_committed_offsets.copy()
