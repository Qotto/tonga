#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os
import logging

from aioevent import AioEvent

from tests.coffee_bar.coffeemaker.models import MakeCoffee, MakeCoffeeResult, CoffeeStarted


if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    log_level = logging.DEBUG
    log_format = '[%(asctime)s] %(levelname)s [%(name)s]: %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_format)
    log = logging.getLogger('kafka')
    log.setLevel(log_level)

    # Init Aio Event without http handler
    aio_event = AioEvent(avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=False)

    # Register command / result / Event
    aio_event.serializer.register_event_class(MakeCoffee,
                                              'aioevent.coffeemaker.command.MakeCoffee')
    aio_event.serializer.register_event_class(MakeCoffeeResult,
                                              'aioevent.coffeemaker.result.MakeCoffeeResult')
    aio_event.serializer.register_event_class(CoffeeStarted,
                                              'aioevent.coffeemaker.event.CoffeeStarted')

    # Create consumer, this consumer read only committed Kafka msg, start point committed
    aio_event.append_consumer('coffee_maker_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id='coffee_maker_1', topics=['coffee-maker-commands'], partition_key=b'0',
                              group_id='coffee_maker', auto_offset_reset='latest', max_retries=10, retry_interval=1000,
                              retry_backoff_coeff=2, isolation_level='read_committed')

    # Create Producer
    aio_event.append_producer('coffee_maker_producer', bootstrap_servers='localhost:9092',
                              client_id='coffee_maker_1', acks=1)

    # Start aioevent (Consumer / Producer)
    aio_event.start()
