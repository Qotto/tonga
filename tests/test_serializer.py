#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from avro.schema import NamedSchema
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata
import asyncio

from aioevent.stores.store_builder.store_builder import StoreBuilder
from aioevent.models.store_record.store_record import StoreRecord
from aioevent.models.store_record.store_record_handler import StoreRecordHandler

from tests.misc.event_class.test_event import TestEvent
from tests.misc.handler_class.test_event_handler import TestEventHandler


def test_init_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    schemas = serializer.get_schemas()

    assert isinstance(schemas['aioevent.store.record'], NamedSchema)
    assert isinstance(schemas['aioevent.event.test'], NamedSchema)


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
    for e_name, dict_class in events.items():
        if e_name.match('aioevent.store.record'):
            assert dict_class['event_class'] == StoreRecord
            assert dict_class['handler_class'] == store_record_handler
            found = True
            break
    assert found


def test_register_base_event_class_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_event_handler = TestEventHandler()
    serializer.register_class('aioevent.event.test', TestEvent, test_event_handler)

    events = serializer.get_events()
    found = False
    for e_name, dict_class in events.items():
        if e_name.match('aioevent.event.test'):
            assert dict_class['event_class'] == TestEvent
            assert dict_class['handler_class'] == test_event_handler
            found = True
            break
    assert found


def test_encode_avro_serializer(get_avro_serializer):
    serializer = get_avro_serializer

    test_encode = TestEvent(test='LOL')
    encoded_test = serializer.encode(test_encode)
    decoded_test = serializer.decode(encoded_test)
    assert test_encode.__dict__ == decoded_test['event_class'].__dict__
    assert decoded_test['handler_class'].handler_name() == 'aioevent.event.test'
