#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest
import asyncio
import os
from avro.schema import NamedSchema
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata

from tonga.stores.store_builder.store_builder import StoreBuilder
from tonga.models.records.store.store_record import StoreRecord
from tonga.models.handlers.store.store_record_handler import StoreRecordHandler
from tonga.services.serializer.avro import AvroSerializer

# TestEvent / TestEventHandler import
from tests.misc.event_class.test_event import TestEvent
from tests.misc.handler_class.test_event_handler import TestEventHandler

# TestCommand / TestCommandHandler import
from tests.misc.event_class.test_command import TestCommand
from tests.misc.handler_class.test_command_handler import TestCommandHandler

# TestResult / TestResultHandler import
from tests.misc.event_class.test_result import TestResult
from tests.misc.handler_class.test_result_handler import TestResultHandler

# Tonga Kafka client
from tonga.services.coordinator.kafka_client.kafka_client import KafkaClient

from tonga.errors import AvroAlreadyRegister, AvroEncodeError


def test_init_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    schemas = serializer.get_schemas()

    assert isinstance(schemas['tonga.store.record'], NamedSchema)
    assert isinstance(schemas['tonga.test.event'], NamedSchema)


def test_register_event_handler_store_record_avro_serializer(get_avro_serializer, get_avro_serializer_store):
    serializer = get_avro_serializer
    local_store, global_store = get_avro_serializer_store

    tonga_kafka_client = KafkaClient(client_id='waiter', cur_instance=0, nb_replica=1,
                                     bootstrap_servers='localhost:9092')
    loop = asyncio.get_event_loop()

    store_builder = StoreBuilder(name='test_store_builder', client=tonga_kafka_client, topic_store='test-store',
                                      serializer=serializer, loop=loop, rebuild=True, event_sourcing=False,
                                      local_store=local_store, global_store=global_store)

    store_record_handler = StoreRecordHandler(store_builder)
    serializer.register_event_handler_store_record(StoreRecord, store_record_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('tonga.store.record'):
            assert event == StoreRecord
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('tonga.store.record'):
            assert handler == store_record_handler
            found = True
            break
    assert found


def test_register_event_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_event_handler = TestEventHandler()
    serializer.register_class('tonga.test.event', TestEvent, test_event_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('tonga.test.event'):
            assert event == TestEvent
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('tonga.test.event'):
            assert handler == test_event_handler
            found = True
            break
    assert found


def test_register_command_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_command_handler = TestCommandHandler()
    serializer.register_class('tonga.test.command', TestCommand, test_command_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('tonga.test.command'):
            assert event == TestCommand
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('tonga.test.command'):
            assert handler == test_command_handler
            found = True
            break
    assert found


def test_register_result_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_result_handler = TestResultHandler()
    serializer.register_class('tonga.test.result', TestResult, test_result_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('tonga.test.result'):
            assert event == TestResult
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('tonga.test.result'):
            assert handler == test_result_handler
            found = True
            break
    assert found


def test_register_more_than_once_avro_serializer():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    with pytest.raises(AvroAlreadyRegister):
        serializer = AvroSerializer(BASE_DIR + '/misc/schemas/bad')


def test_register_bad_event_name_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer
    test_event_handler = TestEventHandler()
    with pytest.raises(NameError):
        serializer.register_class('tonga.qlf', TestEvent, test_event_handler)


def event_name():
    return 'test'


def test_encode_name_error_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_encode = TestEvent(test='LOL')
    test_encode.event_name = event_name
    assert test_encode.event_name() == 'test'
    with pytest.raises(NameError):
        encoded_test = serializer.encode(test_encode)


def test_encode_fail_error_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_encode = TestEvent(test='LOL')
    test_encode.__delattr__('test')
    with pytest.raises(AvroEncodeError):
        encoded_test = serializer.encode(test_encode)


def test_encode_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_encode = TestEvent(test='LOL')

    encoded_test = serializer.encode(test_encode)

    r_dict = serializer.decode(encoded_test)
    assert r_dict['event_class'].__dict__ == test_encode.__dict__
    assert r_dict['handler_class'].handler_name() == 'tonga.test.event'
