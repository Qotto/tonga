#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
from logging.config import dictConfig

from aioevent import AioEvent

from tests.coffee_bar.bartender.models.events import CoffeeFinished
from tests.coffee_bar.bartender.models.commands import MakeCoffee
from tests.coffee_bar.coffeemaker.models.events import CoffeeStarted

if __name__ == '__main__':
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
                'propagate': True,
            },
        }
    }

    dictConfig(LOGGING)

    logger = logging.getLogger(__name__)

    # Initializes Aio Event
    aio_event = AioEvent(avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=False)

    # Registers events
    aio_event.serializer.register_event_class(CoffeeFinished,
                                              'aioevent.bartender.event.CoffeeFinished')
    aio_event.serializer.register_event_class(MakeCoffee,
                                              'aioevent.coffeemaker.command.MakeCoffee')
    aio_event.serializer.register_event_class(CoffeeStarted,
                                              'aioevent.coffeemaker.event.CoffeeStarted')

    # Creates consumer
    aio_event.append_consumer('debug', mod='earliest', bootstrap_servers='localhost:9092',
                              client_id='debug_1', topics=['coffee-maker-events'], auto_offset_reset='earliest',
                              max_retries=10, retry_interval=1000,
                              retry_backoff_coeff=2, isolation_level='read_uncommitted')

    # After build state, start (http handler, consumer, producer)
    aio_event.start()
