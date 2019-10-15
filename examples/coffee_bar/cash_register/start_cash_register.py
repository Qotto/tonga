#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import argparse
import asyncio
import logging
import os
from signal import signal, SIGINT

import uvloop
from colorlog import ColoredFormatter
from sanic import Sanic
from sanic.request import Request

from examples.coffee_bar.cash_register import transactional_manager
from examples.coffee_bar.cash_register.interfaces.rest.cash_register import cash_register_bp
# Import cash register blueprint
from examples.coffee_bar.cash_register.interfaces.rest.health import health_bp
from examples.coffee_bar.cash_register.models.events.bill_created import BillCreated
# Import cash register events
from examples.coffee_bar.cash_register.models.events.bill_paid import BillPaid
from examples.coffee_bar.cash_register.models.events.coffee_ordered import CoffeeOrdered
from examples.coffee_bar.cash_register.models.events.coffee_served import CoffeeServed
# Import cash register handlers
from examples.coffee_bar.cash_register.models.handlers.coffee_ordered_handler import CoffeeOrderedHandler
from examples.coffee_bar.cash_register.models.handlers.coffee_served_handler import CoffeeServedHandler
# Import StoreRecord & StoreRecordHandler
from tonga.models.store.store_record import StoreRecord
from tonga.models.store.store_record_handler import StoreRecordHandler
# Import KafkaProducer / KafkaConsumer
from tonga.services.consumer.kafka_consumer import KafkaConsumer
# Import KafkaClient
from tonga.services.coordinator.client.kafka_client import KafkaClient
# Import key partitioner
from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner
from tonga.services.producer.kafka_producer import KafkaProducer
# Import serializer
from tonga.services.serializer.avro import AvroSerializer
from tonga.stores.global_store.memory import GlobalStoreMemory
# Import local & global store memory
from tonga.stores.local_store.memory import LocalStoreMemory
# Import store builder
from tonga.stores.manager.kafka_store_manager import KafkaStoreManager


def setup_logger():
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s]%(levelname)s: %(name)s/%(module)s/%(funcName)s:%(lineno)d"
        " (%(thread)d) %(blue)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )

    logger = logging.getLogger('tonga')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


if __name__ == '__main__':
    # Argument parser, use for start cash register by instance
    parser = argparse.ArgumentParser(description='Cash register Parser')
    parser.add_argument('instance', metavar='--instance', type=int, help='Service current instance')
    parser.add_argument('nb_replica', metavar='--replica', type=int, help='Replica number')
    parser.add_argument('sanic_port', metavar='--sanic_port', type=int, help='Sanic port')

    args = parser.parse_args()

    cur_instance = args.instance
    nb_replica = args.nb_replica
    sanic_port = args.sanic_port

    try:
        cur_instance = int(cur_instance)
    except ValueError:
        print('Bad instance !')
        exit(-1)

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Creates sanic server
    sanic = Sanic(name=f'cash-register-{cur_instance}')

    # Creates cash register dict app
    cash_register_app = dict()

    cash_register_app['instance'] = cur_instance
    cash_register_app['nb_replica'] = nb_replica

    # Registers logger
    cash_register_app['logger'] = setup_logger()

    cash_register_app['logger'].info(f'Current cash register instance : {cur_instance}')

    # Creates & registers event loop
    cash_register_app['loop'] = uvloop.new_event_loop()
    asyncio.set_event_loop(cash_register_app['loop'])

    # Creates & register Tonga KafkaClient
    cash_register_app['kafka_client'] = KafkaClient(client_id='cash-register', cur_instance=cur_instance,
                                                    nb_replica=nb_replica, bootstrap_servers='localhost:9092')

    cash_register_app['serializer'] = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                  'examples/coffee_bar/avro_schemas'))

    # Creates & registers local store memory / global store memory
    cash_register_app['local_store'] = LocalStoreMemory(name=f'cash-register-{cur_instance}-local-memory')
    cash_register_app['global_store'] = GlobalStoreMemory(name=f'cash-register-{cur_instance}-global-memory')

    # Creates & registers store builder
    cash_register_app['store_builder'] = KafkaStoreManager(name=f'cash-register-{cur_instance}-store-builder',
                                                           client=cash_register_app['kafka_client'],
                                                           topic_store='cash-register-stores',
                                                           serializer=cash_register_app['serializer'],
                                                           local_store=cash_register_app['local_store'],
                                                           global_store=cash_register_app['global_store'],
                                                           loop=cash_register_app['loop'],
                                                           rebuild=True, event_sourcing=False)

    # Creates & register KafkaProducer
    transactional_id = f'cash-register-{cash_register_app["kafka_client"].cur_instance}'
    cash_register_app['transactional_producer'] = KafkaProducer(client=cash_register_app['kafka_client'],
                                                                serializer=cash_register_app['serializer'],
                                                                loop=cash_register_app['loop'],
                                                                partitioner=KeyPartitioner(),
                                                                acks='all',
                                                                transactional_id=transactional_id)

    cash_register_app['transactional_manager'] = transactional_manager
    cash_register_app['transactional_manager'].set_transactional_producer(cash_register_app['transactional_producer'])

    # Initializes cash register handlers
    store_record_handler = StoreRecordHandler(cash_register_app['store_builder'])

    coffee_ordered_handler = CoffeeOrderedHandler(cash_register_app['store_builder'],
                                                  cash_register_app['transactional_producer'])
    coffee_served_handler = CoffeeServedHandler(cash_register_app['store_builder'],
                                                cash_register_app['transactional_producer'])

    # Registers events / handlers in serializer
    cash_register_app['serializer'].register_event_handler_store_record(StoreRecord, store_record_handler)
    cash_register_app['serializer'].register_class('tonga.waiter.event.CoffeeOrdered', CoffeeOrdered,
                                                   coffee_ordered_handler)
    cash_register_app['serializer'].register_class('tonga.waiter.event.CoffeeServed', CoffeeServed,
                                                   coffee_served_handler)
    cash_register_app['serializer'].register_class('tonga.cashregister.event.BillCreated', BillCreated)
    cash_register_app['serializer'].register_class('tonga.cashregister.event.BillPaid', BillPaid)

    # Creates & registers KafkaConsumer
    cash_register_app['consumer'] = KafkaConsumer(client=cash_register_app['kafka_client'],
                                                  serializer=cash_register_app['serializer'],
                                                  topics=['waiter-events'],
                                                  loop=cash_register_app['loop'], group_id='cash-register',
                                                  assignors_data={'instance': cur_instance,
                                                                  'nb_replica': nb_replica,
                                                                  'assignor_policy': 'only_own'},
                                                  isolation_level='read_committed',
                                                  transactional_manager=cash_register_app['transactional_manager'])

    # Ensures future of KafkaConsumer
    asyncio.ensure_future(cash_register_app['consumer'].listen_records('committed'), loop=cash_register_app['loop'])

    # Ensures future of KafkaConsumer store builder
    cash_register_app['store_builder'].return_consumer_task()

    # Attach sanic blueprint
    sanic.blueprint(health_bp)
    sanic.blueprint(cash_register_bp)

    # Creates function for attach cash register to Sanic request
    def attach_cash_register(request: Request):
        request['cash_register'] = cash_register_app

    # Registers cash register middleware
    sanic.register_middleware(attach_cash_register)

    # Creates Sanic server
    server = sanic.create_server(host='0.0.0.0', port=sanic_port, debug=True, access_log=True,
                                 return_asyncio_server=True)

    # Ensures future of Sanic Server
    asyncio.ensure_future(server, loop=cash_register_app['loop'])

    # Catch SIGINT
    signal(SIGINT, lambda s, f: cash_register_app['loop'].stop())
    try:
        # Runs forever
        cash_register_app['loop'].run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        cash_register_app['loop'].stop()
