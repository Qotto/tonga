#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import asyncio
import uvloop
import os
from asyncio import AbstractEventLoop
from signal import signal, SIGINT
from logging import Logger
from sanic import Sanic

from typing import Dict, Any

from .base import BaseApp
from aioevent.services import BaseSerializer, AvroSerializer, BaseConsumer, BaseProducer, KafkaConsumer, KafkaProducer

__all__ = [
    'AioEvent',
]


class AioEvent(BaseApp):
    sanic_host: str
    sanic_port: int
    sanic_handler_name: str
    sanic_access_log: bool
    sanic_debug: bool
    avro_schemas_folder: str
    serializer: BaseSerializer
    logger: Logger
    consumers: Dict[str, BaseConsumer]
    producers: Dict[str, BaseProducer]
    _loop: AbstractEventLoop

    def __init__(self, sanic_host: str = '127.0.0.1', sanic_port: int = 8000, http_handler: bool = True,
                 sanic_handler_name: str = 'default_handler', avro_schemas_folder: str = None,
                 sanic_access_log: bool = False, sanic_debug: bool = False):
        self.sanic_host = sanic_host
        self.sanic_port = sanic_port
        self.sanic_handler_name = sanic_handler_name
        self.sanic_access_log = sanic_access_log
        self.sanic_debug = sanic_debug
        if avro_schemas_folder is None:
            self.avro_schemas_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'avro_schemas')
        else:
            self.avro_schemas_folder = avro_schemas_folder

        self.logger = logging.getLogger(__name__)

        self.consumers = dict()
        self.producers = dict()

        if http_handler:
            self.http_handler = Sanic(name=self.sanic_handler_name)
        else:
            self.http_handler = None

        self.serializer = AvroSerializer(self.avro_schemas_folder)
        self._loop = uvloop.new_event_loop()
        asyncio.set_event_loop(self._loop)

    def start(self) -> None:
        if self.http_handler is not None:
            self.http_handler.request_middleware.append(self.__attach_aioevent_to_http_request)

            server = self.http_handler.create_server(host=self.sanic_host, port=self.sanic_port, debug=self.sanic_debug,
                                                     access_log=self.sanic_access_log, return_asyncio_server=True)
            asyncio.ensure_future(server, loop=self._loop)
        signal(SIGINT, lambda s, f: self._loop.stop())
        try:
            self._loop.run_forever()
        except Exception:
            self._loop.stop()

    def add_task(self, task) -> None:
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
        return self._loop

    def append_consumer(self, consumer_name: str, mod: str = 'latest', listen_event: bool = True,
                        start_before_server: bool = False, **kwargs) -> None:
        self.consumers[consumer_name] = KafkaConsumer(app=self, serializer=self.serializer, **kwargs)
        if start_before_server:
            print('Before Start ')
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.consumers[consumer_name].listen_building_state())
            print('Before end')
        elif listen_event:
            asyncio.ensure_future(self.consumers[consumer_name].listen_event(mod), loop=self._loop)

    def append_producer(self, producer_name: str, **kwargs) -> None:
        self.producers[producer_name] = KafkaProducer(serializer=self.serializer, **kwargs)

    def __attach_aioevent_to_http_request(self, request) -> None:
        request['aio_event'] = self

    def attach(self, object_name: str, object_instance: object) -> None:
        self.__setattr__(object_name, object_instance)

    def get(self, object_name: str) -> Any:
        return self.__getattribute__(object_name)
