#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
from colorlog import ColoredFormatter
import argparse
import uvloop
import asyncio
from signal import signal, SIGINT

# Import KafkaProducer / KafkaConsumer
from aioevent.services.consumer.kafka_consumer import KafkaConsumer
from aioevent.services.producer.kafka_producer import KafkaProducer
# Import serializer
from aioevent.services.serializer.avro import AvroSerializer
# Import key partitioner
from aioevent.services.coordinator.partitioner.key_partitioner import KeyPartitioner

# Import coffee-maker events
from examples.coffee_bar.coffeemaker.models.results.make_coffee_result import MakeCoffeeResult
from examples.coffee_bar.coffeemaker.models.commands.make_coffee import MakeCoffee
from examples.coffee_bar.coffeemaker.models.events.coffee_started import CoffeeStarted
# Import coffee-maker handlers
from examples.coffee_bar.coffeemaker.models.handlers.coffee_started_handler import CoffeeStartedHandler
from examples.coffee_bar.coffeemaker.models.handlers.make_coffee_result_handler import MakeCoffeeResultHandler
from examples.coffee_bar.coffeemaker.models.handlers.make_coffee_handler import MakeCoffeeHandler


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

    logger = logging.getLogger('aioevent')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


if __name__ == '__main__':
    # Argument parser, use for start waiter by instance
    parser = argparse.ArgumentParser(description='Coffee-maker Parser')
    parser.add_argument('instance', metavar='--instance', type=int, help='Service current instance')
    parser.add_argument('nb_replica', metavar='--replica', type=int, help='Replica number')

    args = parser.parse_args()

    cur_instance = args.instance
    nb_replica = args.nb_replica

    try:
        cur_instance = int(cur_instance)
    except ValueError:
        print('Bad instance !')
        exit(-1)

    # Creates coffee-maker dict app
    coffee_maker_app = dict()

    # Register coffee-maker info
    coffee_maker_app['instance'] = cur_instance
    coffee_maker_app['nb_replica'] = nb_replica

    # Registers logger
    coffee_maker_app['logger'] = setup_logger()

    coffee_maker_app['logger'].info(f'Coffee-maker current instance : {cur_instance}')

    # Creates & registers event loop
    coffee_maker_app['loop'] = uvloop.new_event_loop()
    asyncio.set_event_loop(coffee_maker_app['loop'])

    # Creates & registers Avro serializer
    coffee_maker_app['serializer'] = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                                 'examples/coffee_bar/avro_schemas'))

    # Creates & register KafkaProducer
    coffee_maker_app['producer'] = KafkaProducer(name=f'coffee-maker-{cur_instance}',
                                                 bootstrap_servers='localhost:9092',
                                                 client_id=f'coffee-maker-{cur_instance}',
                                                 serializer=coffee_maker_app['serializer'],
                                                 loop=coffee_maker_app['loop'], partitioner=KeyPartitioner(),
                                                 acks='all', transactional_id=f'coffee-maker')

    # Initializes coffee-maker handlers
    make_coffee_handler = MakeCoffeeHandler(coffee_maker_app['producer'])
    make_coffee_result_handler = MakeCoffeeResultHandler()
    coffee_started_handler = CoffeeStartedHandler()

    # Registers events / handlers in serializer
    coffee_maker_app['serializer'].register_class('aioevent.coffeemaker.command.MakeCoffee', MakeCoffee,
                                                  make_coffee_handler)
    coffee_maker_app['serializer'].register_class('aioevent.coffeemaker.result.MakeCoffeeResult', MakeCoffeeResult,
                                                  make_coffee_result_handler)
    coffee_maker_app['serializer'].register_class('aioevent.coffeemaker.event.CoffeeStarted', CoffeeStarted,
                                                  coffee_started_handler)

    # Creates & registers KafkaConsumer
    coffee_maker_app['consumer'] = KafkaConsumer(name=f'coffee-maker-{cur_instance}',
                                                 serializer=coffee_maker_app['serializer'],
                                                 bootstrap_servers='localhost:9092',
                                                 client_id=f'coffee-maker-{cur_instance}',
                                                 topics=['coffee-maker-commands'],
                                                 loop=coffee_maker_app['loop'], group_id='coffee-maker',
                                                 assignors_data={'instance': cur_instance,
                                                                 'nb_replica': nb_replica,
                                                                 'assignor_policy': 'only_own'},
                                                 isolation_level='read_committed')

    # Ensures future of KafkaConsumer
    asyncio.ensure_future(coffee_maker_app['consumer'].listen_event('committed'), loop=coffee_maker_app['loop'])

    # Catch SIGINT
    signal(SIGINT, lambda s, f: coffee_maker_app['loop'].stop())
    try:
        # Runs forever
        coffee_maker_app['loop'].run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        coffee_maker_app['loop'].stop()
