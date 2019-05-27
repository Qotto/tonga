#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
import argparse
import uvloop
import asyncio
from sanic import Sanic
from sanic.request import Request
from logging.config import dictConfig
from signal import signal, SIGINT
from kafka import KafkaAdminClient
from kafka.cluster import ClusterMetadata

# Import KafkaProducer / KafkaConsumer
from aioevent.services.consumer.kafka_consumer import KafkaConsumer
from aioevent.services.producer.kafka_producer import KafkaProducer
# Import serializer
from aioevent.services.serializer.avro import AvroSerializer
# Import local & global store memory
from aioevent.stores.local.memory import LocalStoreMemory
from aioevent.stores.globall.memory import GlobalStoreMemory
# Import store builder
from aioevent.stores.store_builder.store_builder import StoreBuilder
# Import key partitioner
from aioevent.services.coordinator.partitioner.key_partitioner import KeyPartitioner

# Import waiter blueprint
from examples.coffee_bar.waiter.interfaces.rest.health import health_bp
from examples.coffee_bar.waiter.interfaces.rest.waiter import waiter_bp
# Import waiter events
from examples.coffee_bar.waiter.models.events.coffee_ordered import CoffeeOrdered
from examples.coffee_bar.waiter.models.events.coffee_finished import CoffeeFinished
from examples.coffee_bar.waiter.models.events.coffee_served import CoffeeServed
# Import waiter handlers
from examples.coffee_bar.waiter.models.handlers.coffee_ordered_handler import CoffeeOrderedHandler
from examples.coffee_bar.waiter.models.handlers.coffee_finished_handler import CoffeeFinishedHandler
from examples.coffee_bar.waiter.models.handlers.coffee_served_handler import CoffeeServedHandler


if __name__ == '__main__':
    # Argument parser, use for start waiter by instance
    parser = argparse.ArgumentParser(description='Waiter Parser')
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

    # Logger configuration

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'verbose': {
                'format': '[%(asctime)s] %(levelname)s: %(name)s/%(module)s/%(funcName)s:'
                          '%(lineno)d (%(thread)d) %(message)s'
            },
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'verbose',
            },
        },
        'loggers': {
            'aioevent': {
                'level': 'DEBUG',
                'handlers': ['console'],
                'propagate': False,
            },
        }
    }

    dictConfig(LOGGING)

    # Creates sanic server
    sanic = Sanic(name=f'waiter-{cur_instance}')

    # Creates waiter dict app
    waiter_app = dict()

    # Register waiter app info
    waiter_app['instance'] = cur_instance
    waiter_app['nb_replica'] = nb_replica

    # Registers logger
    waiter_app['logger'] = logging.getLogger(__name__)

    waiter_app['logger'].info('Hello my name is Albert !\nWaiter current instance : {cur_instance}')

    # Creates & registers event loop
    waiter_app['loop'] = uvloop.new_event_loop()
    asyncio.set_event_loop(waiter_app['loop'])

    waiter_app['serializer'] = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                           'tests/coffee_bar/avro_schemas'))

    # Creates & registers local store memory / global store memory

    waiter_app['local_store'] = LocalStoreMemory(name=f'waiter-{cur_instance}-local-memory')
    waiter_app['global_store'] = GlobalStoreMemory(name=f'waiter-{cur_instance}-global-memory')

    cluster_admin = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id='test_store_builder')
    cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')

    # Creates & registers store builder
    waiter_app['store_builder'] = StoreBuilder(name=f'waiter-{cur_instance}-store-builder',
                                               current_instance=cur_instance, nb_replica=nb_replica,
                                               topic_store='waiter-stores', serializer=waiter_app['serializer'],
                                               local_store=waiter_app['local_store'],
                                               global_store=waiter_app['global_store'],
                                               bootstrap_server='localhost:9092', cluster_metadata=cluster_metadata,
                                               cluster_admin=cluster_admin, loop=waiter_app['loop'], rebuild=True,
                                               event_sourcing=False)

    # Initializes waiter handlers
    coffee_ordered_handler = CoffeeOrderedHandler()
    coffee_finished_handler = CoffeeFinishedHandler(waiter_app['store_builder'])
    coffee_served_handler = CoffeeServedHandler()

    # Registers events / handlers in serializer
    waiter_app['serializer'].register_class('aioevent.waiter.event.CoffeeOrdered', CoffeeOrdered,
                                            coffee_ordered_handler)
    waiter_app['serializer'].register_class('aioevent.bartender.event.CoffeeFinished', CoffeeFinished,
                                            coffee_finished_handler)
    waiter_app['serializer'].register_class('aioevent.waiter.event.CoffeeServed', CoffeeServed,
                                            coffee_served_handler)

    # Creates & registers KafkaConsumer
    waiter_app['consumer'] = KafkaConsumer(name=f'waiter-{cur_instance}', serializer=waiter_app['serializer'],
                                           bootstrap_servers='localhost:9092', client_id=f'waiter-{cur_instance}',
                                           topics=['waiter-events', 'bartender-events'],
                                           loop=waiter_app['loop'], group_id='waiter',
                                           assignors_data={'instance': cur_instance,
                                                           'nb_replica': nb_replica,
                                                           'assignor_policy': 'only_own'},
                                           isolation_level='read_committed')

    # Ensures future of KafkaConsumer
    asyncio.ensure_future(waiter_app['consumer'].listen_event('committed'), loop=waiter_app['loop'])

    # Creates & register KafkaProducer
    waiter_app['producer'] = KafkaProducer(name=f'waiter-{cur_instance}', bootstrap_servers='localhost:9092',
                                           client_id=f'waiter-{cur_instance}', serializer=waiter_app['serializer'],
                                           loop=waiter_app['loop'], partitioner=KeyPartitioner(), acks='all',
                                           transactional_id=f'waiter')

    # Attach sanic blueprint
    sanic.blueprint(health_bp)
    sanic.blueprint(waiter_bp)

    # Creates function for attach waiter to Sanic request
    def attach_waiter(request: Request):
        request['waiter'] = waiter_app

    # Registers waiter middleware
    sanic.register_middleware(attach_waiter)

    # Creates Sanic server
    server = sanic.create_server(host='0.0.0.0', port=sanic_port, debug=True, access_log=True,
                                 return_asyncio_server=True)
    # Ensures future of Sanic Server
    asyncio.ensure_future(server, loop=waiter_app['loop'])

    # Catch SIGINT
    signal(SIGINT, lambda s, f: waiter_app['loop'].stop())
    try:
        # Runs forever
        waiter_app['loop'].run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        waiter_app['loop'].stop()
