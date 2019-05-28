#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
from colorlog import ColoredFormatter
import argparse
import uvloop
import asyncio
from sanic.request import Request
from signal import signal, SIGINT


# Import KafkaProducer / KafkaConsumer
from aioevent.services.consumer.kafka_consumer import KafkaConsumer
from aioevent.services.producer.kafka_producer import KafkaProducer
# Import serializer
from aioevent.services.serializer.avro import AvroSerializer
# Import key partitioner
from aioevent.services.coordinator.partitioner.key_partitioner import KeyPartitioner

# Import bartender events
from examples.coffee_bar.bartender.models.events.coffee_finished import CoffeeFinished
from examples.coffee_bar.bartender.models.events.bill_created import BillCreated
from examples.coffee_bar.bartender.models.events.bill_paid import BillPaid
from examples.coffee_bar.bartender.models.commands.make_coffee import MakeCoffee
from examples.coffee_bar.bartender.models.results.make_coffee_result import MakeCoffeeResult
# Import bartender handlers
from examples.coffee_bar.bartender.models.handlers.coffee_finished_handler import CoffeeFinishedHandler
from examples.coffee_bar.bartender.models.handlers.bill_created_handler import BillCreatedHandler
from examples.coffee_bar.bartender.models.handlers.bill_paid_handler import BillPaidHandler
from examples.coffee_bar.bartender.models.handlers.make_coffee_handler import MakeCoffeeHandler
from examples.coffee_bar.bartender.models.handlers.make_coffee_result_handler import MakeCoffeeResultHandler


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
    parser = argparse.ArgumentParser(description='Bartender Parser')
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

    # Creates bartender dict app
    bartender_app = dict()

    # Register bartender app info
    bartender_app['instance'] = cur_instance
    bartender_app['nb_replica'] = nb_replica

    # Registers logger
    bartender_app['logger'] = setup_logger()

    bartender_app['logger'].info(f'Bartender current instance : {cur_instance}')

    # Creates & registers event loop
    bartender_app['loop'] = uvloop.new_event_loop()
    asyncio.set_event_loop(bartender_app['loop'])

    bartender_app['serializer'] = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                              'examples/coffee_bar/avro_schemas'))

    # Creates & register KafkaProducer
    bartender_app['producer'] = KafkaProducer(name=f'bartender-{cur_instance}', bootstrap_servers='localhost:9092',
                                              client_id=f'bartender-{cur_instance}',
                                              serializer=bartender_app['serializer'], loop=bartender_app['loop'],
                                              partitioner=KeyPartitioner(), acks='all', transactional_id=f'bartender')

    # Initializes bartender handlers
    coffee_finished_handler = CoffeeFinishedHandler()
    bill_created_handler = BillCreatedHandler(bartender_app['producer'])
    bill_paid_handler = BillPaidHandler()
    make_coffee_handler = MakeCoffeeHandler()
    make_coffee_result_handler = MakeCoffeeResultHandler(bartender_app['producer'])

    # Registers events / handlers in serializer
    bartender_app['serializer'].register_class('aioevent.coffeemaker.command.MakeCoffee', MakeCoffee,
                                               make_coffee_handler)
    bartender_app['serializer'].register_class('aioevent.cashregister.event.BillPaid', BillPaid,
                                               bill_paid_handler)
    bartender_app['serializer'].register_class('aioevent.coffeemaker.result.MakeCoffeeResult', MakeCoffeeResult,
                                               make_coffee_result_handler)
    bartender_app['serializer'].register_class('aioevent.cashregister.event.BillCreated', BillCreated,
                                               bill_created_handler)
    bartender_app['serializer'].register_class('aioevent.bartender.event.CoffeeFinished', CoffeeFinished,
                                               coffee_finished_handler)

    # Creates & registers KafkaConsumer
    bartender_app['consumer'] = KafkaConsumer(name=f'bartender-{cur_instance}', serializer=bartender_app['serializer'],
                                              bootstrap_servers='localhost:9092', client_id=f'bartender-{cur_instance}',
                                              topics=['cash-register-events', 'coffee-maker-results'], loop=bartender_app['loop'],
                                              group_id='bartender', assignors_data={'instance': cur_instance,
                                                                                    'nb_replica': nb_replica,
                                                                                    'assignor_policy': 'only_own'},
                                              isolation_level='read_committed')

    # Ensures future of KafkaConsumer
    asyncio.ensure_future(bartender_app['consumer'].listen_event('committed'), loop=bartender_app['loop'])

    # Creates function for attach bartender to Sanic request
    def attach_bartender(request: Request):
        request['bartender'] = bartender_app

    # Catch SIGINT
    signal(SIGINT, lambda s, f: bartender_app['loop'].stop())
    try:
        # Runs forever
        bartender_app['loop'].run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        bartender_app['loop'].stop()
