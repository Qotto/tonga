#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Regular packages

Import tonga component
"""

__version__ = '0.0.1'

# Import KafkaConsumer / KafkaProducer
from .services.consumer.kafka_consumer import KafkaConsumer
from .services.producer.kafka_producer import KafkaProducer

# Import AvroSerializer
from .services.serializer.avro import AvroSerializer

# Import KeyPartitioner
from .services.coordinator.partitioner.key_partitioner import KeyPartitioner

# Import BaseEvent / BaseCommand / BaseResult
from .models.records.base import BaseRecord
from .models.records.event.event import BaseEvent
from .models.records.command.command import BaseCommand
from .models.records.result.result import BaseResult

# Import BaseEventHandler / BaseCommandHandler / BaseResultHandler
from .models.handlers.event.event_handler import BaseEventHandler
from .models.handlers.command.command_handler import BaseCommandHandler
from .models.handlers.result.result_handler import BaseResultHandler

# Import StoreRecord / StoreRecordHandler
from .models.records.store.store_record import StoreRecord
from tonga.models.handlers.store.store_record_handler import StoreRecordHandler

# Import StoreBuilder
from .stores.store_builder.store_builder import StoreBuilder

# Import LocalStoreMemory / GlobalStoreMemory
from .stores.local.memory import LocalStoreMemory
from .stores.globall.memory import GlobalStoreMemory


__all__ = [
    # KafkaConsumer / KafkaProducer
    'KafkaConsumer',
    'KafkaProducer',
    # AvroSerializer
    'AvroSerializer',
    # KeyPartitioner
    'KeyPartitioner',
    # BaseEvent / BaseCommand / BaseResult
    'BaseRecord',
    'BaseEvent',
    'BaseCommand',
    'BaseResult',
    # BaseEventHandler / BaseCommandHandler / BaseResultHandler
    'BaseEventHandler',
    'BaseCommandHandler',
    'BaseResultHandler',
    # StoreRecord / StoreRecordHandler
    'StoreRecord',
    'StoreRecordHandler',
    # StoreBuilder
    'StoreBuilder',
    # LocalStoreMemory / GlobalStoreMemory
    'LocalStoreMemory',
    'GlobalStoreMemory'
]
