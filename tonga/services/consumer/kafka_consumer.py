#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" KafkaConsumer class

This class consume event / command / result in Kafka topics.

Todo:
    * In function listen_store_records, add commit store (later V2)
"""

import asyncio
import json
from logging import Logger, getLogger
from typing import List, Dict, Any, Union

from aiokafka import (AIOKafkaConsumer)
from aiokafka.errors import (IllegalStateError, UnsupportedVersionError, CommitFailedError,
                             KafkaError, KafkaTimeoutError)
from kafka.errors import KafkaConnectionError

from tonga.models.handlers.command.command_handler import BaseCommandHandler
from tonga.models.handlers.event.event_handler import BaseEventHandler
from tonga.models.handlers.result.result_handler import BaseResultHandler
from tonga.models.records.base import BaseRecord
from tonga.models.store.base import BaseStoreRecordHandler
from tonga.models.store.store_record import StoreRecord
from tonga.services.consumer.base import BaseConsumer
from tonga.services.consumer.errors import (ConsumerConnectionError, AioKafkaConsumerBadParams,
                                            KafkaConsumerError, ConsumerKafkaTimeoutError,
                                            IllegalOperation, TopicPartitionError,
                                            NoPartitionAssigned, OffsetError, UnknownStoreRecordHandler,
                                            UnknownHandler, UnknownHandlerReturn,
                                            HandlerException, KafkaConsumerAlreadyStartedError,
                                            KafkaConsumerNotStartedError)
from tonga.services.coordinator.assignors.statefulset_assignors import StatefulsetPartitionAssignor
from tonga.services.coordinator.client.kafka_client import KafkaClient
from tonga.services.coordinator.transaction.kafka_transaction import (KafkaTransactionalManager,
                                                                      KafkaTransactionContext)
from tonga.services.errors import BadSerializer
from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.kafka_key import KafkaKeySerializer
from tonga.stores.manager.base import BaseStoreManager
from tonga.stores.manager.errors import UninitializedStore
from tonga.models.structs.positioning import (BasePositioning, KafkaPositioning)

__all__ = [
    'KafkaConsumer',
]


class KafkaConsumer(BaseConsumer):
    """KafkaConsumer is a client that publishes records to the Kafka cluster.
    """
    _client: KafkaClient
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
    _store_builder: BaseStoreManager
    _running: bool
    _kafka_consumer: AIOKafkaConsumer
    _transactional_manager: KafkaTransactionalManager

    __current_offsets: Dict[str, BasePositioning]
    __last_offsets: Dict[str, BasePositioning]
    __last_committed_offsets: Dict[str, BasePositioning]

    _loop: asyncio.AbstractEventLoop
    logger: Logger

    def __init__(self, client: KafkaClient, serializer: BaseSerializer, topics: List[str],
                 loop: asyncio.AbstractEventLoop, client_id: str = None, group_id: str = None,
                 auto_offset_reset: str = 'earliest', max_retries: int = 10, retry_interval: int = 1000,
                 retry_backoff_coeff: int = 2, assignors_data: Dict[str, Any] = None,
                 store_builder: BaseStoreManager = None, isolation_level: str = 'read_uncommitted',
                 transactional_manager: KafkaTransactionalManager = None) -> None:
        """
        KafkaConsumer constructor

        Args:
            client (KafkaClient): Initialization class (contains, client_id / bootstraps_server)
            serializer (BaseSerializer): Serializer encode & decode event
            topics (List[str]): List of topics to subscribe to
            loop (asyncio.AbstractEventLoop): Asyncio loop
            client_id (str): Client name (if is none, KafkaConsumer use KafkaClient client_id)
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
        self.logger = getLogger('tonga')

        # Register KafkaClient
        self._client = client

        # Set default assignors_data if is None
        if assignors_data is None:
            assignors_data = {}

        # Create client_id
        if client_id is None:
            self._client_id = self._client.client_id + '-' + str(self._client.cur_instance)
        else:
            self._client_id = client_id

        if isinstance(serializer, BaseSerializer):
            self.serializer = serializer
        else:
            raise BadSerializer

        self._bootstrap_servers = self._client.bootstrap_servers
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

        self._transactional_manager = transactional_manager

        try:
            self.logger.info(json.dumps(assignors_data))
            statefulset_assignor = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data), 'utf-8'))
            self._kafka_consumer = AIOKafkaConsumer(*self._topics, loop=self._loop,
                                                    bootstrap_servers=self._bootstrap_servers,
                                                    client_id=self._client_id, group_id=group_id,
                                                    value_deserializer=self.serializer.decode,
                                                    auto_offset_reset=self._auto_offset_reset,
                                                    isolation_level=self._isolation_level, enable_auto_commit=False,
                                                    key_deserializer=KafkaKeySerializer.decode,
                                                    partition_assignment_strategy=[statefulset_assignor])
        except KafkaError as err:
            self.logger.exception('%s', err.__str__())
            raise err
        except ValueError as err:
            self.logger.exception('%s', err.__str__())
            raise AioKafkaConsumerBadParams
        self.logger.debug('Create new consumer %s, group_id %s', self._client_id, group_id)

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
                self.logger.debug('Start consumer : %s, group_id : %s, retry : %s', self._client_id, self._group_id,
                                  retry)
            except KafkaTimeoutError as err:
                self.logger.exception('%s', err.__str__())
                await asyncio.sleep(1)
            except KafkaConnectionError as err:
                self.logger.exception('%s', err.__str__())
                await asyncio.sleep(1)
            except KafkaError as err:
                self.logger.exception('%s', err.__str__())
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
            self.logger.debug('Stop consumer : %s, group_id : %s', self._client_id, self._group_id)
        except KafkaTimeoutError as err:
            self.logger.exception('%s', err.__str__())
            raise ConsumerKafkaTimeoutError
        except KafkaError as err:
            self.logger.exception('%s', err.__str__())
            raise err

    def is_running(self) -> bool:
        return self._running

    async def get_last_committed_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get last committed offsets

        Returns:
            Dict[str, KafkaPositioning]: Contains all assigned partitions with last committed offsets
        """
        last_committed_offsets: Dict[str, BasePositioning] = dict()
        self.logger.debug('Get last committed offsets')
        if self._group_id is None:
            raise IllegalOperation
        for tp in self._kafka_consumer.assignment():
            offset = await self._kafka_consumer.committed(tp)
            last_committed_offsets[KafkaPositioning.make_class_assignment_key(tp.topic, tp.partition)] = \
                KafkaPositioning(tp.topic, tp.partition, offset)
        return last_committed_offsets

    async def get_current_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get current offsets

        Returns:
            Dict[str, KafkaPositioning]: Contains all assigned partitions with current offsets
        """
        current_offsets: Dict[str, BasePositioning] = dict()
        self.logger.debug('Get current offsets')
        for tp in self._kafka_consumer.assignment():
            try:
                offset = await self._kafka_consumer.position(tp)
                current_offsets[KafkaPositioning.make_class_assignment_key(tp.topic, tp.partition)] = \
                    KafkaPositioning(tp.topic, tp.partition, offset)
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise err
        return current_offsets

    async def get_beginning_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get beginning offsets

        Returns:
            Dict[str, KafkaPositioning]: Contains all assigned partitions with beginning offsets
        """
        beginning_offsets: Dict[str, BasePositioning] = dict()
        self.logger.debug('Get beginning offsets')
        for tp in self._kafka_consumer.assignment():
            try:
                offset = (await self._kafka_consumer.beginning_offsets([tp]))[tp]
                beginning_offsets[KafkaPositioning.make_class_assignment_key(tp.topic, tp.partition)] = \
                    KafkaPositioning(tp.topic, tp.partition, offset)
            except KafkaTimeoutError as err:
                self.logger.exception('%s', err.__str__())
                raise ConsumerKafkaTimeoutError
            except UnsupportedVersionError as err:
                self.logger.exception('%s', err.__str__())
                raise err
        return beginning_offsets

    async def get_last_offsets(self) -> Dict[str, BasePositioning]:
        """
        Get last offsets

        Returns:
            Dict[str, KafkaPositioning]: Contains all assigned partitions with last offsets
        """
        last_offsets: Dict[str, BasePositioning] = dict()
        self.logger.debug('Get last offsets')
        for tp in self._kafka_consumer.assignment():
            try:
                offset = (await self._kafka_consumer.end_offsets([tp]))[tp]
                last_offsets[KafkaPositioning.make_class_assignment_key(tp.topic, tp.partition)] = \
                    KafkaPositioning(tp.topic, tp.partition, offset)
            except KafkaTimeoutError as err:
                self.logger.exception('%s', err.__str__())
                raise ConsumerKafkaTimeoutError
            except UnsupportedVersionError as err:
                self.logger.exception('%s', err.__str__())
                raise err
        return last_offsets

    async def load_offsets(self, mod: str = 'earliest') -> None:
        """
        This method was call before consume topics, assign position to consumer

        Args:
            mod: Start position of consumer (earliest, latest, committed)

        Returns:
            None
        """
        self.logger.debug('Load offset mod : %s', mod)
        if not self._running:
            await self.start_consumer()

        if mod == 'latest':
            await self.seek_to_end()
        elif mod == 'earliest':
            await self.seek_to_beginning()
        elif mod == 'committed':
            await self.seek_to_last_commit()
        else:
            raise KafkaConsumerError

        self.__current_offsets = await self.get_current_offsets()
        self.__last_offsets = await self.get_last_offsets()

        if self._group_id is not None:
            self.__last_committed_offsets = await self.get_last_committed_offsets()
            for key, kafka_positioning in self.__last_committed_offsets.items():
                if kafka_positioning.get_current_offset() is None:
                    self.logger.debug('Seek to beginning, no committed offsets was found')
                    await self.seek_to_beginning(kafka_positioning)

    async def debug_print_all_msg(self):
        """
        Debug method, useful for display all msg was contained in assigned topic/partitions

        Returns:
            None
        """
        while True:
            message = await self._kafka_consumer.getone()
            self.logger.info('----------------------------------------------------------------------------------------')
            self.logger.info('Topic %s, Partition %s, Offset %s, Key %s, Value %s, Headers %s',
                             message.topic, message.partition, message.offset, message.key, message.value,
                             message.headers)
            self.logger.info('----------------------------------------------------------------------------------------')

    async def listen_records(self, mod: str = 'earliest') -> None:
        """
        Listens records from assigned topic / partitions

        Args:
            mod: Start position of consumer (earliest, latest, committed)

        Returns:
            None
        """
        if not self._running:
            await self.load_offsets(mod)

        self.pprint_consumer_offsets()

        async for msg in self._kafka_consumer:
            # Debug Display
            self.logger.debug("---------------------------------------------------------------------------------")
            self.logger.debug('New Message on consumer %s, Topic %s, Partition %s, Offset %s, '
                              'Key %s, Value %s, Headers %s', self._client_id, msg.topic, msg.partition,
                              msg.offset, msg.key, msg.value, msg.headers)
            self.pprint_consumer_offsets()
            self.logger.debug("---------------------------------------------------------------------------------")

            key = KafkaPositioning.make_class_assignment_key(msg.topic, msg.partition)
            self.__current_offsets[key].set_current_offset(msg.offset)
            if self._transactional_manager is not None:
                self._transactional_manager.set_ctx(KafkaTransactionContext(msg.topic, msg.partition,
                                                                            msg.offset, self._group_id))
            # self.last_offsets = await self.get_last_offsets()

            sleep_duration_in_ms = self._retry_interval
            for retries in range(0, self._max_retries):
                try:
                    record_class = msg.value['record_class']
                    handler_class = msg.value['handler_class']

                    if handler_class is None:
                        self.logger.debug('Empty handler')
                        break

                    self.logger.debug('Event name : %s  Event content :\n%s',
                                      record_class.event_name(), record_class.__dict__)

                    # Calls handle if event is instance BaseHandler
                    if isinstance(handler_class, BaseEventHandler):
                        transactional = await handler_class.handle(event=record_class)
                    elif isinstance(handler_class, BaseCommandHandler):
                        transactional = await handler_class.execute(event=record_class)
                    elif isinstance(handler_class, BaseResultHandler):
                        transactional = await handler_class.on_result(event=record_class)
                    else:
                        # Otherwise raise KafkaConsumerUnknownHandler
                        raise UnknownHandler

                    # If result is none (no transactional process), check if consumer has an
                    # group_id (mandatory to commit in Kafka)
                    if transactional is None and self._group_id is not None:
                        # Check if next commit was possible (Kafka offset)
                        if self.__last_committed_offsets[key] is None or \
                                self.__last_committed_offsets[key].get_current_offset() <= \
                                self.__current_offsets[key].get_current_offset():

                            self.logger.debug('Commit msg %s in topic %s partition %s offset %s',
                                              record_class.event_name(), msg.topic, msg.partition,
                                              self.__current_offsets[key].get_current_offset() + 1)
                            tp = self.__current_offsets[key].to_topics_partition()
                            await self._kafka_consumer.commit(
                                {tp: self.__current_offsets[key].get_current_offset() + 1})
                            self.__last_committed_offsets[key].set_current_offset(msg.offset + 1)

                    # Transactional process no commit
                    elif transactional:
                        self.logger.debug('Transaction end')
                        self.__current_offsets = await self.get_current_offsets()
                        self.__last_committed_offsets = await self.get_last_committed_offsets()
                    # Otherwise raise KafkaConsumerUnknownHandlerReturn
                    elif transactional is None and self._group_id is None:
                        pass
                    else:
                        raise UnknownHandlerReturn

                    # Break if everything was successfully processed
                    break
                except UninitializedStore as err:
                    self.logger.exception('%s', err.__str__())
                    retries = 0
                    await asyncio.sleep(10)
                except IllegalStateError as err:
                    self.logger.exception('%s', err.__str__())
                    raise NoPartitionAssigned
                except ValueError as err:
                    self.logger.exception('%s', err.__str__())
                    raise OffsetError
                except CommitFailedError as err:
                    self.logger.exception('%s', err.__str__())
                    raise err
                except (KafkaError, HandlerException) as err:
                    self.logger.exception('%s', err.__str__())
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self._retry_backoff_coeff
                    if retries not in range(0, self._max_retries):
                        await self.stop_consumer()
                        self.logger.error('Max retries, close consumer and exit')
                        exit(1)

    async def check_if_store_is_ready(self) -> None:
        """ If store is ready consumer set store initialize flag to true

        Returns:
            None
        """

        # Check if local store is initialize
        self.logger.info('Started check_if_store_is_ready')
        if not self._store_builder.get_local_store().is_initialized():
            local_store_metadata = await self._store_builder.get_local_store().get_metadata()
            local_store_positioning = local_store_metadata.assigned_partitions[0]
            if self.__last_offsets[local_store_positioning.make_assignment_key()].get_current_offset() == 0:
                self._store_builder.get_local_store().set_initialized(True)
                self.logger.info('Local store was initialized')
            elif local_store_metadata.last_offsets[local_store_positioning.make_assignment_key()].get_current_offset() \
                    == (self.__last_offsets[local_store_positioning.make_assignment_key()].get_current_offset()):
                self._store_builder.get_local_store().set_initialized(True)
                self.logger.info('Local store was initialized')

        # Check if global store is initialize
        if not self._store_builder.get_global_store().is_initialized():
            global_store_metadata = await self._store_builder.get_global_store().get_metadata()
            for key, positioning in self.__last_offsets.items():
                if self._client.cur_instance != positioning.get_partition():
                    if positioning.get_current_offset() == 0:
                        continue
                    elif (positioning.get_current_offset()) == \
                            global_store_metadata.last_offsets[positioning.make_assignment_key()].get_current_offset():
                        continue
                    else:
                        break
            else:
                self._store_builder.get_global_store().set_initialized(True)
                self.logger.info('Global store was initialized')

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
        await self._store_builder.initialize_store_manager()

        if not self._running:
            raise KafkaConsumerError('Fail to start tongaConsumer', 500)

        # Check if store is ready
        await self.check_if_store_is_ready()
        self.pprint_consumer_offsets()

        async for msg in self._kafka_consumer:
            positioning_key = KafkaPositioning.make_class_assignment_key(msg.topic, msg.partition)
            self.__current_offsets[positioning_key].set_current_offset(msg.offset)

            # Debug Display
            self.logger.debug("---------------------------------------------------------------------------------")
            self.logger.debug('New Message on consumer %s, Topic %s, Partition %s, Offset %s, '
                              'Key %s, Value %s, Headers %s', self._client_id, msg.topic, msg.partition,
                              msg.offset, msg.key, msg.value, msg.headers)
            self.pprint_consumer_offsets()
            self.logger.debug("---------------------------------------------------------------------------------")

            # Check if store is ready
            await self.check_if_store_is_ready()

            sleep_duration_in_ms = self._retry_interval
            for retries in range(0, self._max_retries):
                try:
                    record_class: BaseRecord = msg.value['record_class']
                    handler_class: BaseStoreRecordHandler = msg.value['handler_class']

                    self.logger.debug('Store event name : %s\nEvent content :\n%s\n',
                                      record_class.event_name(), record_class.__dict__)

                    positioning = self.__current_offsets[positioning_key]
                    if self._store_builder.get_current_instance() == msg.partition:
                        # Calls local_state_handler if event is instance BaseStorageBuilder
                        if rebuild and not self._store_builder.get_local_store().is_initialized():
                            if isinstance(record_class, StoreRecord):
                                self.logger.debug('Call local_store_handler')
                                await handler_class.local_store_handler(store_record=record_class,
                                                                        positioning=positioning)
                            else:
                                raise UnknownStoreRecordHandler
                    elif self._store_builder.get_current_instance() != msg.partition:
                        if isinstance(record_class, StoreRecord):
                            self.logger.debug('Call global_store_handler')
                            await handler_class.global_store_handler(store_record=record_class, positioning=positioning)
                        else:
                            raise UnknownStoreRecordHandler

                    self.logger.info('Before next check store !')

                    # Check if store is ready

                    await self.check_if_store_is_ready()

                    # Break if everything was successfully processed
                    break
                except IllegalStateError as err:
                    self.logger.exception('%s', err.__str__())
                    raise NoPartitionAssigned
                except ValueError as err:
                    self.logger.exception('%s', err.__str__())
                    raise OffsetError
                except CommitFailedError as err:
                    self.logger.exception('%s', err.__str__())
                    raise err
                except (KafkaError, HandlerException) as err:
                    self.logger.exception('%s', err.__str__())
                    sleep_duration_in_s = int(sleep_duration_in_ms / 1000)
                    await asyncio.sleep(sleep_duration_in_s)
                    sleep_duration_in_ms = sleep_duration_in_ms * self._retry_backoff_coeff
                    if retries not in range(0, self._max_retries):
                        await self.stop_consumer()
                        self.logger.error('Max retries, close consumer and exit')
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

    async def seek_to_beginning(self, positioning: BasePositioning = None) -> None:
        """
        Seek to fist offset, mod 'earliest'.
        If positioning is None consumer will seek all assigned partition to beginning

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number)

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if positioning is not None:
            try:
                await self._kafka_consumer.seek_to_beginning(positioning.to_topics_partition())
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception('%s', err.__str__())
                raise TopicPartitionError
            self.logger.debug('Seek to beginning for topic : %s, partition : %s', positioning.get_partition(),
                              positioning.get_partition())
        else:
            try:
                await self._kafka_consumer.seek_to_beginning()
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            self.logger.debug('Seek to beginning for all topics & partitions')

    async def seek_to_end(self, positioning: BasePositioning = None) -> None:
        """
        Seek to latest offset, mod 'latest'.
        If positioning is None consumer will seek all assigned partition to end

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number)

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if positioning is not None:
            try:
                await self._kafka_consumer.seek_to_end(positioning.to_topics_partition())
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception('%s', err.__str__())
                raise TopicPartitionError
            self.logger.debug('Seek to end for topic : %s, partition : %s', positioning.get_topics(),
                              positioning.get_partition())
        else:
            try:
                await self._kafka_consumer.seek_to_end()
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            self.logger.debug('Seek to end for all topics & partitions')

    async def seek_to_last_commit(self, positioning: BasePositioning = None) -> None:
        """
        Seek to last committed offsets, mod 'committed'
        If positioning is None consumer will seek all assigned partition to last committed offset

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number / offset number)

        Returns:
            None
        """
        if self._group_id is None:
            raise IllegalOperation
        if not self._running:
            await self.start_consumer()
        if positioning:
            try:
                await self._kafka_consumer.seek_to_committed(positioning.to_topics_partition())
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            except TypeError as err:
                self.logger.exception('%s', err.__str__())
                raise TopicPartitionError
            self.logger.debug('Seek to last committed for topic : %s, partition : %s', positioning.get_topics(),
                              positioning.get_partition())
        else:
            try:
                await self._kafka_consumer.seek_to_committed()
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            self.logger.debug('Seek to last committed for all topics & partitions')

    async def seek_custom(self, positioning: BasePositioning) -> None:
        """
        Seek to custom offsets

        Args:
            positioning (BasePositioning): Positioning class contain (topic name / partition number / offset number)

        Returns:
            None
        """
        if not self._running:
            await self.start_consumer()
        if positioning is not None:
            try:
                await self._kafka_consumer.seek(positioning.to_topics_partition(), positioning.get_current_offset())
            except ValueError as err:
                self.logger.exception('%s', err.__str__())
                raise OffsetError
            except TypeError as err:
                self.logger.exception('%s', err.__str__())
                raise TopicPartitionError
            except IllegalStateError as err:
                self.logger.exception('%s', err.__str__())
                raise NoPartitionAssigned
            self.logger.debug('Custom seek for topic : %s, partition : %s, offset : %s',
                              positioning.get_topics(), positioning.get_partition(), positioning.get_current_offset())
        else:
            raise KafkaConsumerError

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
        self.logger.debug('Client ID = %s', self._client_id)

        self.logger.debug('Current Offset = %s', [positioning.pprint() for key, positioning in
                                                  self.__current_offsets.items()])
        self.logger.debug('Last Offset = %s', [positioning.pprint() for key, positioning in
                                               self.__last_offsets.items()])

        self.logger.debug('Last committed offset = %s', [positioning.pprint() for key, positioning in
                                                         self.__last_committed_offsets.items()])

    def get_consumer(self) -> AIOKafkaConsumer:
        """
        Get aiokafka consumer

        Returns:
            AIOKafkaConsumer: Current instance of AIOKafkaConsumer
        """
        return self._kafka_consumer

    def get_offset_bundle(self) -> Dict[str, Dict[str, BasePositioning]]:
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

    def get_current_offset(self) -> Dict[str, BasePositioning]:
        """
        Return current offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains current offset of each assigned partition
        """
        return self.__current_offsets.copy()

    def get_last_offset(self) -> Dict[str, BasePositioning]:
        """
        Return last offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains latest offset of each assigned partition
        """
        return self.__last_offsets.copy()

    def get_last_committed_offset(self) -> Dict[str, BasePositioning]:
        """
        Return last committed offset of each assigned topic/partition

        Returns:
            Dict[str, BasePositioning]: Dict contains last committed offset of each assigned partition
        """
        return self.__last_committed_offsets.copy()
