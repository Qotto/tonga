#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging
from logging.config import dictConfig

from aioevent import AioEvent

from examples.coffee_bar.coffeemaker.models import MakeCoffee, MakeCoffeeResult, CoffeeStarted


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
                'propagate': False,
            },
        }
    }

    dictConfig(LOGGING)

    logger = logging.getLogger(__name__)

    # Initializes Aio Event without http handler
    aio_event = AioEvent(avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=False)

    # Registers command / result / Event
    aio_event.serializer.register_event_class(MakeCoffee,
                                              'aioevent.coffeemaker.command.MakeCoffee')
    aio_event.serializer.register_event_class(MakeCoffeeResult,
                                              'aioevent.coffeemaker.result.MakeCoffeeResult')
    aio_event.serializer.register_event_class(CoffeeStarted,
                                              'aioevent.coffeemaker.event.CoffeeStarted')

    # Creates consumer, this consumer read only committed Kafka msg, start point committed
    aio_event.append_consumer('coffee_maker_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id='coffee_maker_1', topics=['coffee-maker-commands'], group_id='coffee_maker',
                              auto_offset_reset='latest', max_retries=10, retry_interval=1000, retry_backoff_coeff=2,
                              isolation_level='read_committed')

    # Creates Transactional Producer, (isolation_level parameter) this consumer only return transactional messages which
    # have been committed, non-transactional messages will be returned unconditionally in either mode.
    aio_event.append_producer('coffee_maker_transactional_producer', bootstrap_servers='localhost:9092',
                              client_id='coffee_maker_1', acks='all', transactional_id='coffeemaker_transactional')

    # Starts aioevent (Consumer / Producer)
    aio_event.start()
