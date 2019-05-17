#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
import argparse
from logging.config import dictConfig

from aioevent import AioEvent

from tests.coffee_bar.waiter.repository.waiter.shelf import ShelfWaiterRepository
from tests.coffee_bar.waiter.interfaces.rest.health import health_bp
from tests.coffee_bar.waiter.interfaces.rest.waiter import waiter_bp
from tests.coffee_bar.waiter.models import CoffeeOrdered, CoffeeFinished, CoffeeServed


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

    print('Hello my name is Albert ! ')

    try:
        cur_instance = int(cur_instance)
    except ValueError:
        print('Bad instance !')
        exit(-1)

    print(f'Waiter current instance : {cur_instance}')

    # Initializes Aio Event
    aio_event = AioEvent(sanic_host='0.0.0.0', sanic_port=sanic_port, sanic_handler_name=f'waiter_{cur_instance}',
                         avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=True, sanic_access_log=True, sanic_debug=True, instance=cur_instance)

    # Attaches waiter repository (db)
    aio_event.attach('waiter_local_repository', ShelfWaiterRepository(
        os.path.join(BASE_DIR, f'local_state_waiter_{cur_instance}.repository')))
    aio_event.attach('waiter_global_repository', ShelfWaiterRepository(
        os.path.join(BASE_DIR, f'global_state_waiter_{cur_instance}.repository')))

    # Registers events
    aio_event.serializer.register_event_class(CoffeeOrdered,
                                              'aioevent.waiter.event.CoffeeOrdered')
    aio_event.serializer.register_event_class(CoffeeFinished,
                                              'aioevent.bartender.event.CoffeeFinished')
    aio_event.serializer.register_event_class(CoffeeServed,
                                              'aioevent.waiter.event.CoffeeServed')

    # Creates consumer
    aio_event.append_consumer('waiter_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id=f'waiter_{cur_instance}', topics=['waiter-events', 'bartender-events'],
                              group_id=f'waiter_{cur_instance}', auto_offset_reset='latest', max_retries=10,
                              retry_interval=1000, retry_backoff_coeff=2,
                              assignors_data={'instance': cur_instance, 'nb_replica': nb_replica,
                                              'assignor_policy': 'all'},
                              state_builder=True, isolation_level='read_committed')

    # Creates producer
    aio_event.append_producer('waiter_producer', bootstrap_servers='localhost:9092', client_id=f'waiter_{cur_instance}',
                              acks=1)

    # Registers waiter blueprint
    aio_event.http_handler.register_blueprint(health_bp)
    aio_event.http_handler.register_blueprint(waiter_bp)

    # After build state, start (http handler, consumer, producer)
    aio_event.start()
