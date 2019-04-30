#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import os

from aioevent import AioEvent

from tests.coffee_bar.cash_register.interfaces.rest.health import health_bp
from tests.coffee_bar.cash_register.interfaces.rest.cash_register import cash_register_bp

from tests.coffee_bar.cash_register.state.cash_register import CashRegister
from tests.coffee_bar.cash_register.models import BillCreated, BillPaid, CoffeeServed, CoffeeOrdered


if __name__ == '__main__':
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # Init Aio Event
    aio_event = AioEvent(sanic_host='0.0.0.0', sanic_port=8001, sanic_handler_name='cash_register',
                         avro_schemas_folder=os.path.join(BASE_DIR, 'tests/coffee_bar/avro_schemas'),
                         http_handler=True, sanic_access_log=True, sanic_debug=True)

    # Register my event
    aio_event.serializer.register_event_class(BillCreated,
                                              'aioevent.cashregister.event.BillCreated')
    aio_event.serializer.register_event_class(BillPaid,
                                              'aioevent.cashregister.event.BillPaid')
    aio_event.serializer.register_event_class(CoffeeServed,
                                              'aioevent.waiter.event.CoffeeServed')
    aio_event.serializer.register_event_class(CoffeeOrdered,
                                              'aioevent.waiter.event.CoffeeOrdered')

    # Attach Cash Register State
    aio_event.attach('cash_register_state', CashRegister())

    # Build State when running
    aio_event.append_consumer('cash_register_state_consumer', mod='earliest', bootstrap_servers='localhost:9092',
                              client_id='cash_register_state', topics=['cash-register-events'], partition_key=b'0',
                              auto_offset_reset='earliest', max_retries=10, retry_interval=1000, retry_backoff_coeff=2)

    # Consumer Waiter event
    aio_event.append_consumer('cash_register_consumer', mod='committed', bootstrap_servers='localhost:9092',
                              client_id='cash_register', topics=['waiter-events'],
                              partition_key=b'0', group_id='cash_register', auto_offset_reset='earliest', max_retries=10,
                              retry_interval=1000, retry_backoff_coeff=2, isolation_level='read_committed')

    # Producer Event
    aio_event.append_producer('cash_register_producer', bootstrap_servers='localhost:9092',
                              client_id='cash_register', acks=1)

    # Register Sanic Event
    aio_event.http_handler.register_blueprint(health_bp)
    aio_event.http_handler.register_blueprint(cash_register_bp)

    # After build my state, call start (Consumer / Producer / Http Handle)
    aio_event.start()
