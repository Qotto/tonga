#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest
import asyncio
import os
from avro.schema import NamedSchema
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata

from aioevent.stores.store_builder.store_builder import StoreBuilder
from aioevent.models.store_record.store_record import StoreRecord
from aioevent.models.store_record.store_record_handler import StoreRecordHandler
from aioevent.services.serializer.avro import AvroSerializer

# TestEvent / TestEventHandler import
from tests.misc.event_class.test_event import TestEvent
from tests.misc.handler_class.test_event_handler import TestEventHandler

# TestCommand / TestCommandHandler import
from tests.misc.event_class.test_command import TestCommand
from tests.misc.handler_class.test_command_handler import TestCommandHandler

# TestResult / TestResultHandler import
from tests.misc.event_class.test_result import TestResult
from tests.misc.handler_class.test_result_handler import TestResultHandler

from aioevent.models.exceptions import AvroAlreadyRegister, AvroDecodeError, AvroEncodeError


def test_init_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    schemas = serializer.get_schemas()

    assert isinstance(schemas['aioevent.store.record'], NamedSchema)
    assert isinstance(schemas['aioevent.test.event'], NamedSchema)


def test_register_event_handler_store_record_avro_serializer(get_avro_serializer, get_avro_serializer_store):
    serializer = get_avro_serializer
    local_store, global_store = get_avro_serializer_store

    admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='test_store_builder')
    cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')
    loop = asyncio.get_event_loop()
    store_builder = StoreBuilder('test_store_builder', 0, 1, 'test-store', serializer, local_store, global_store,
                                 'localhost:9092', cluster_metadata, admin_client, loop, False, False)

    store_record_handler = StoreRecordHandler(store_builder)
    serializer.register_event_handler_store_record(StoreRecord, store_record_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('aioevent.store.record'):
            assert event == StoreRecord
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('aioevent.store.record'):
            assert handler == store_record_handler
            found = True
            break
    assert found


def test_register_event_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_event_handler = TestEventHandler()
    serializer.register_class('aioevent.test.event', TestEvent, test_event_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('aioevent.test.event'):
            assert event == TestEvent
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('aioevent.test.event'):
            assert handler == test_event_handler
            found = True
            break
    assert found


def test_register_command_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_command_handler = TestCommandHandler()
    serializer.register_class('aioevent.test.command', TestCommand, test_command_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('aioevent.test.command'):
            assert event == TestCommand
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('aioevent.test.command'):
            assert handler == test_command_handler
            found = True
            break
    assert found


def test_register_result_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_result_handler = TestResultHandler()
    serializer.register_class('aioevent.test.result', TestResult, test_result_handler)

    events = serializer.get_events()
    found = False
    for e_name, event in events.items():
        if e_name.match('aioevent.test.result'):
            assert event == TestResult
            found = True
            break
    assert found

    found = False
    handlers = serializer.get_handlers()
    for e_name, handler in handlers.items():
        if e_name.match('aioevent.test.result'):
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
        serializer.register_class('aioevent.qlf', TestEvent, test_event_handler)


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
    decoded_test, handler_test = serializer.decode(encoded_test)
    assert test_encode.__dict__ == decoded_test.__dict__
    assert handler_test.handler_name() == 'aioevent.test.event'
