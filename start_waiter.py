#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os

from aioevent import AioEvent

from tests.coffee_bar.waiter.states.waiter import WaiterState
from tests.coffee_bar.waiter.interfaces.rest.health import health_bp
from tests.coffee_bar.waiter.interfaces.rest.waiter import waiter_bp
from tests.coffee_bar.waiter.models import CoffeeOrdered, CoffeeFinished, CoffeeServed


if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    print('Hello my name is albert ! ')

    # Init Aio Event
    aio_event = AioEvent(sanic_host='0.0.0.0', sanic_port=8000, sanic_handler_name='waiter',
                         avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=True, sanic_access_log=True, sanic_debug=True)

    # Attach waiter state
    aio_event.attach('waiter_state', WaiterState())

    # Create Event
    aio_event.serializer.register_event_class(CoffeeOrdered,
                                              'aioevent.waiter.event.CoffeeOrdered')
    aio_event.serializer.register_event_class(CoffeeFinished,
                                              'aioevent.bartender.event.CoffeeFinished')
    aio_event.serializer.register_event_class(CoffeeServed,
                                              'aioevent.waiter.event.CoffeeServed')

    # Create Consumer for state building
    aio_event.append_consumer('waiter_consumer_state', start_before_server=True, mod='earliest', listen_event=False,
                              bootstrap_servers='localhost:9092', client_id='waiter_1',
                              topics=['waiter-events', 'bartender-events'], auto_offset_reset='earliest',
                              max_retries=10, retry_interval=1000, retry_backoff_coeff=2,
                              isolation_level='read_uncommitted')
    # Build waiter state
    aio_event.get('waiter_state').init_state()

    # Create consumer
    aio_event.append_consumer('waiter_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id='waiter_1', topics=['bartender-events'],
                              group_id='waiter', auto_offset_reset='latest', max_retries=10, retry_interval=1000,
                              retry_backoff_coeff=2, isolation_level='read_uncommitted')

    # Create producer
    aio_event.append_producer('waiter_producer', bootstrap_servers='localhost:9092', client_id='waiter_1', acks=1)

    # Register waiter blueprint
    aio_event.http_handler.register_blueprint(health_bp)
    aio_event.http_handler.register_blueprint(waiter_bp)

    # After build state, start (http handler, consumer, producer)
    aio_event.start()
