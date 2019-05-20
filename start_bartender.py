#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import argparse
import logging
from logging.config import dictConfig

from aioevent import AioEvent

from examples.coffee_bar.bartender.models import MakeCoffee, MakeCoffeeResult, CoffeeFinished, BillCreated, BillPaid

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Waiter Parser')
    parser.add_argument('instance', metavar='--instance', type=int, help='Service current instance')
    parser.add_argument('nb_replica', metavar='--replica', type=int, help='Replica number')

    args = parser.parse_args()

    cur_instance = args.instance
    nb_replica = args.nb_replica

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    LOGGING = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'verbose': {
                'format': '[%(asctime)s] %(levelname)s: %(name)s/%(module)s/%(funcName)s:%(lineno)d (%(thread)d) %(message)s'
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

    logger = logging.getLogger(__name__)

    # Initializes Aio Event
    aio_event = AioEvent(avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=False, instance=cur_instance)

    # Registers my event
    aio_event.serializer.register_event_class(MakeCoffee,
                                              'aioevent.coffeemaker.command.MakeCoffee')
    aio_event.serializer.register_event_class(BillPaid,
                                              'aioevent.cashregister.event.BillPaid')
    aio_event.serializer.register_event_class(MakeCoffeeResult,
                                              'aioevent.coffeemaker.result.MakeCoffeeResult')
    aio_event.serializer.register_event_class(BillCreated,
                                              'aioevent.cashregister.event.BillCreated')
    aio_event.serializer.register_event_class(CoffeeFinished,
                                              'aioevent.bartender.event.CoffeeFinished')

    # Creates consumer, (isolation_level parameter) this consumer only return transactional messages which
    # have been committed, non-transactional messages will be returned unconditionally in either mode.
    aio_event.append_consumer('bartender_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id=f'bartender_{cur_instance}', group_id=f'bartender_{cur_instance}',
                              topics=['cash-register-events', 'coffee-maker-results'],
                              auto_offset_reset='latest', max_retries=10, state_builder=False,
                              assignors_data={'instance': cur_instance, 'nb_replica': nb_replica,
                                              'assignor_policy': 'only_own'},
                              retry_interval=1000, retry_backoff_coeff=2, isolation_level='read_committed')

    # Creates producer
    aio_event.append_producer('bartender_producer', bootstrap_servers='localhost:9092', client_id='bartender_1', acks=1)

    # Creates transactional producer
    aio_event.append_producer('bartender_transactional_producer', bootstrap_servers='localhost:9092',
                              client_id='bartender_2', acks='all', transactional_id='bartender_transactional')

    # Starts (producer / consumer)
    aio_event.start()
