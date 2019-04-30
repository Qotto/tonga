#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os

from aioevent import AioEvent

from tests.coffee_bar.bartender.models import MakeCoffee, MakeCoffeeResult, CoffeeFinished, BillCreated

if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Init Aio Event
    aio_event = AioEvent(avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=False)

    # Register my event
    aio_event.serializer.register_event_class(MakeCoffee,
                                              'aioevent.coffeemaker.command.MakeCoffee')
    aio_event.serializer.register_event_class(MakeCoffeeResult,
                                              'aioevent.coffeemaker.result.MakeCoffeeResult')
    aio_event.serializer.register_event_class(BillCreated,
                                              'aioevent.cashregister.event.BillCreated')
    aio_event.serializer.register_event_class(CoffeeFinished,
                                              'aioevent.bartender.event.CoffeeFinished')

    # Create consumer, this consumer read only committed Kafka msg, start point committed
    aio_event.append_consumer('bartender_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id='bartender_1', topics=['cash-register-events', 'coffee-maker-results'],
                              partition_key=b'0', group_id='bartender', auto_offset_reset='latest', max_retries=10,
                              retry_interval=1000, retry_backoff_coeff=2, isolation_level='read_committed')

    # Create producer
    aio_event.append_producer('bartender_producer', bootstrap_servers='localhost:9092', client_id='bartender_1',
                              acks=1)

    # Start (producer / consumer)
    aio_event.start()
