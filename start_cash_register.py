#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import argparse
import logging
from logging.config import dictConfig

from aioevent import AioEvent

from examples.coffee_bar.cash_register.interfaces.rest.health import health_bp
from examples.coffee_bar.cash_register.interfaces.rest.cash_register import cash_register_bp

from examples.coffee_bar.cash_register.repository.cash_register.shelf import ShelfCashRegisterRepository
from examples.coffee_bar.cash_register.models import BillCreated, BillPaid, CoffeeServed, CoffeeOrdered


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Waiter Parser')
    parser.add_argument('instance', metavar='--instance', type=int, help='Service current instance')
    parser.add_argument('nb_replica', metavar='--replica', type=int, help='Replica number')
    parser.add_argument('sanic_port', metavar='--sanic_port', type=int, help='Sanic port')

    args = parser.parse_args()

    cur_instance = args.instance
    nb_replica = args.nb_replica
    sanic_port = args.sanic_port

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
    aio_event = AioEvent(sanic_host='0.0.0.0', sanic_port=sanic_port,
                         sanic_handler_name=f'cash_register_{cur_instance}',
                         avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=True, sanic_access_log=True, sanic_debug=True, instance=cur_instance)

    # Registers events
    aio_event.serializer.register_event_class(BillCreated,
                                              'aioevent.cashregister.event.BillCreated')
    aio_event.serializer.register_event_class(BillPaid,
                                              'aioevent.cashregister.event.BillPaid')
    aio_event.serializer.register_event_class(CoffeeServed,
                                              'aioevent.waiter.event.CoffeeServed')
    aio_event.serializer.register_event_class(CoffeeOrdered,
                                              'aioevent.waiter.event.CoffeeOrdered')

    # Attaches cash register local & global repository
    aio_event.attach('cash_register_local_repository', ShelfCashRegisterRepository(
        os.path.join(BASE_DIR, f'local_cash_register_{cur_instance}.repository')))
    aio_event.attach('cash_register_global_repository', ShelfCashRegisterRepository(
        os.path.join(BASE_DIR, f'global_cash_register_{cur_instance}.repository')))

    # Creates consumer
    aio_event.append_consumer('cash_register_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id=f'cash_register_{cur_instance}', group_id=f'cash_register_{cur_instance}',
                              topics=['cash-register-events', 'waiter-events'], auto_offset_reset='latest',
                              max_retries=10, retry_interval=1000, retry_backoff_coeff=2, state_builder=True,
                              assignors_data={'instance': cur_instance, 'nb_replica': nb_replica,
                                              'assignor_policy': 'all'},
                              isolation_level='read_uncommitted')

    # Creates producer
    aio_event.append_producer('cash_register_producer', bootstrap_servers='localhost:9092',
                              client_id=f'cash_register_{cur_instance}', acks=1)

    # Registers Sanic blueprint
    aio_event.http_handler.register_blueprint(health_bp)
    aio_event.http_handler.register_blueprint(cash_register_bp)

    # Starts (Consumer / Producer / Http Handle)
    aio_event.start()
