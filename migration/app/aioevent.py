#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import asyncio
import uvloop
import os
from kafka.cluster import ClusterMetadata
from kafka.admin import KafkaAdminClient
from aiokafka import TopicPartition
from asyncio import AbstractEventLoop
from signal import signal, SIGINT
from logging import Logger
from sanic import Sanic
from sanic.request import Request

from typing import Dict, Any, List, Union, Callable, Coroutine

from .base import BaseApp
from aioevent.services.serializer import BaseSerializer, AvroSerializer
from aioevent.services.consumer import BaseConsumer, AioeventConsumer
from aioevent.services.producer import BaseProducer, AioeventProducer
from aioevent.stores.store_builder import StoreBuilder
from aioevent.stores import BaseLocalStore
from aioevent.stores import BaseGlobalStore

__all__ = [
    'AioEvent',
]


class AioEvent(BaseApp):
    """
    AioEvent class, this is the main library class

    Attributes:
        name (str): AioEvent instance name, use your project name
        _sanic_host (str): Sanic host IP
        _sanic_port (int): Sanic port
        _sanic_access_log (bool): Disable or enable sanic logging
        _sanic_debug (bool): Disable or enable sanic debug
        _avro_schemas_folder (str): Folder with all avro schemas
        serializer (BaseSerializer): Serializer used by consumer & producer & store
        _consumers (Dict[str, KafkaConsumer]): Dict of KafkaConsumer by name
        _producers (Dict[str, KafkaProducer]): Dict of KafkaProducer by name
        _stores_builder (Dict[str, StoreBuilder]): Dict of StoreBuilder by name
        _nb_replica (int): Total number of replicas
        _instance (int): Current instance
        _cluster_metadata (ClusterMetadata): ClusterMetadata from kafka-python
        _cluster_admin (KafkaAdminClient): KafkaAdminClient from kafka-python
        _assigned_topics (List[TopicPartition]): List of assigned topics
        _stores_topics (List[TopicPartition]): List of stores topics
        logger (Logger): Aioevent logger
        _loop (AbstractEventLoop): Asyncio event loop
    """
    name: str
    _sanic_host: str
    _sanic_port: int
    _sanic_access_log: bool
    _sanic_debug: bool

    _avro_schemas_folder: str
    serializer: BaseSerializer

    _consumers: Dict[str, AioeventConsumer]
    _producers: Dict[str, AioeventProducer]

    _stores_builder: Dict[str, StoreBuilder]

    _nb_replica: int
    _instance: int

    _cluster_metadata: ClusterMetadata
    _cluster_admin: KafkaAdminClient

    _assigned_topics: List[TopicPartition]
    _stores_topics: List[TopicPartition]

    logger: Logger
    _loop: AbstractEventLoop

    def __init__(self, name: str, instance: int = 0, nb_replica: int = 0, sanic_host: str = '127.0.0.1',
                 sanic_port: int = 8000, http_handler: bool = True,
                 bootstrap_servers: Union[str, List[str]] = '127.0.0.1:9092',
                 avro_schemas_folder: str = None, sanic_access_log: bool = False, sanic_debug: bool = False) -> None:
        """
        This method instantiates Aioevent class, this is the main class of library

        Args:
            name (str): Aioevent name, use your project name, used for build some internal named
            instance (int): Current instance of running project
            nb_replica (int): Total instance number, by default
            sanic_host (str): Sanic host IP, by default Sanic host is '127.0.0.1'
            sanic_port (int): Sanic network port, by default Sanic port is '8000'
            http_handler (bool): If true aioevent create an http handler with sanic, otherwise no http handler
            bootstrap_servers (str): ‘host[:port]’ string (or list of ‘host[:port]’ strings) that the consumer
                                      should contact to bootstrap initial cluster metadata
            avro_schemas_folder (str): Path to avro schemas folder, if is none aioevent try to find avro folder
                                       in your root project folder
            sanic_access_log (bool): Disable sanic logging, by default is False
            sanic_debug (bool): Disable or enable Sanic debug, by default is false

        Returns:
            None
        """
        self.name = name
        self._sanic_host = sanic_host
        self._sanic_port = sanic_port
        self._sanic_access_log = sanic_access_log
        self._sanic_debug = sanic_debug

        if avro_schemas_folder is None:
            self._avro_schemas_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'avro_schemas')
        else:
            self._avro_schemas_folder = avro_schemas_folder

        self._instance = instance
        self._nb_replica = nb_replica
        self.logger = logging.getLogger(__name__)

        self._consumers = dict()
        self._producers = dict()
        self._stores_builder = dict()
        self._assigned_topics = list()
        self._stores_topics = list()

        if http_handler:
            self._http_handler = Sanic(name=f'{self.name}-{self._instance}')
        else:
            self._http_handler = None

        self.serializer = AvroSerializer(self._avro_schemas_folder)
        self._loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self._loop)

        self._cluster_admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                               client_id=f'{self.name}-{self._instance}')
        self._cluster_metadata = ClusterMetadata(bootstrap_servers=bootstrap_servers)

    # noinspection PyBroadException
    def start(self) -> None:
        """
        This method start aioevent, call all start function form consumer & producer & StoreBuilder

        Returns:
            None
        """
        if self._http_handler is not None:
            self._http_handler.request_middleware.append(self.__attach_aioevent_to_http_request)

            server = self._http_handler.create_server(host=self._sanic_host, port=self._sanic_port,
                                                      debug=self._sanic_debug, access_log=self._sanic_access_log,
                                                      return_asyncio_server=True)
            asyncio.ensure_future(server, loop=self._loop)
        signal(SIGINT, lambda s, f: self._loop.stop())
        try:
            self._loop.run_forever()
        except Exception:
            self._loop.stop()

    def append_store_builder(self, store_builder_name: str, topic_store: str, local_store: BaseLocalStore,
                             global_store: BaseGlobalStore, bootstrap_server: str, rebuild: bool,
                             event_sourcing: bool = False) -> StoreBuilder:
        """
        Append a new store builder, this class create a new store_consumer & store_producer.

        Args:
            store_builder_name (str): Name of store builder, used for get local or global storage
            topic_store (str): Topic name to store state event
            local_store (BaseLocalStore): Local store, Memory / Shelve / RockDB
            global_store (BaseGlobalStore): Global store, Memory / Shelve / RockerDB
            bootstrap_server (str): Kafka server host & ip
            rebuild (bool): Optional param, set to true for build store from fist topic partition offset
            event_sourcing (bool): If this flag is true as true Aioevent turn on event_sourcing StoreBuilder

        Returns:
            StoreBuilder instance
        """
        self._stores_builder[store_builder_name] = StoreBuilder(store_builder_name=store_builder_name,
                                                                current_instance=self._instance,
                                                                nb_replica=self._nb_replica, topic_store=topic_store,
                                                                local_store=local_store, global_store=global_store,
                                                                serializer=self.serializer,
                                                                bootstrap_server=bootstrap_server,
                                                                cluster_metadata=self._cluster_metadata,
                                                                cluster_admin=self._cluster_admin, loop=self._loop,
                                                                rebuild=rebuild, event_sourcing=event_sourcing)
        return self._stores_builder[store_builder_name]

    def append_consumer(self, consumer_name: str, mod: str = 'latest', listen_event: bool = True,
                        **kwargs: str) -> BaseConsumer:
        """
        Append a new consumer, this function create and register consumer in dict, get with get_consumer method

        Args:
            consumer_name (str): Consumer name, used for get consumer by name
            mod (str): Start position of consumer (earliest, latest, committed)
            listen_event (str): If true aioevent ensure future of consumer, otherwise consumer doesn't consume event
            **kwargs (str): Config for consumer, please see docstring of KafkaConsumer class

        Returns:
            BaseConsumer instance
        """
        self._consumers[consumer_name] = AioeventConsumer(app=self, serializer=self.serializer, loop=self._loop, **kwargs)  # type: ignore
        if listen_event:
            asyncio.ensure_future(self._consumers[consumer_name].listen_event(mod), loop=self._loop)
        return self._consumers[consumer_name]

    def append_producer(self, producer_name: str, **kwargs: str) -> BaseProducer:
        """
        Append a new producer, this function create and register a producer in dict, get with get_producer method

        Args:
            producer_name (str): Producer name, used for get producer by name
            **kwargs (str): Config for producer, please go to docstring of KafkaProducer class

        Returns:
            BaseProducer instance
        """
        self._producers[producer_name] = AioeventProducer(serializer=self.serializer, loop=self._loop, **kwargs)
        return self._producers[producer_name]

    def add_task(self, task: Union[Callable, Coroutine]) -> None:
        """
        Add task in aioevent

        Args:
            task (Union[Callable, Coroutine]): Callable or coroutine method is needed for run task in loop

        Returns:
            None
        """
        try:
            if callable(task):
                try:
                    self._loop.create_task(task(self))
                except TypeError:
                    self._loop.create_task(task())
            else:
                self._loop.create_task(task)
        except Exception:
            raise RuntimeError

    def return_asyncio_loop(self) -> AbstractEventLoop:
        """
        Return Asyncio event Loop

        Returns:
            AbstractEventLoop
        """
        return self._loop

    def __attach_aioevent_to_http_request(self, request: Request) -> None:
        """
        Add a new middleware on Sanic, attach self in request.

        Args:
            request (Request): Sanic request module

        Returns:
            None
        """
        request['aio_event'] = self

    def get_global_store(self, store_builder_name: str) -> BaseGlobalStore:
        """
        Returns an global store by store builder name

        Args:
            store_builder_name (str): StoreBuilder name

        Returns:
            BaseGlobalStore
        """
        return self._stores_builder[store_builder_name].get_global_store()

    def get_local_store(self, store_builder_name: str) -> BaseLocalStore:
        """
        Returns an local store by store builder name

        Args:
            store_builder_name (str): StoreBuilder name

        Returns:
            BaseLocalStore
        """
        return self._stores_builder[store_builder_name].get_local_store()

    def get_store_builder(self, store_builder_name: str) -> StoreBuilder:
        """
        Returns an store builder by name

        Args:
            store_builder_name (str): StoreBuilder name

        Returns:
            StoresBuilder
        """
        return self._stores_builder[store_builder_name]

    def get_producer(self, producer_name: str) -> BaseProducer:
        """
        Returns an producer by name

        Args:
            producer_name: BaseProducer name

        Returns:
            BaseProducer
        """
        return self._producers[producer_name]

    def get_consumer(self, consumer_name: str) -> BaseConsumer:
        """
        Returns an consumer by name

        Args:
            consumer_name: BaseConsumer Name

        Returns:
            BaseConsumer
        """
        return self._consumers[consumer_name]

    def attach(self, object_name: str, object_instance: object) -> None:
        """
        Attach custom class on aioevent

        Args:
            object_name (str): Object name
            object_instance (object): Object instance

        Returns:
            None
        """
        self.__setattr__(object_name, object_instance)

    def get(self, object_name: str) -> Any:
        """
        Get aioevent attribute

        Args:
            object_name (str): Object name

        Returns:
            Any
        """
        return self.__getattribute__(object_name)
