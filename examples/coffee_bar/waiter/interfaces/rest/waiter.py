#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse

from tonga.stores.errors import StoreKeyNotFound
from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner

from examples.coffee_bar.waiter.models.events import CoffeeOrdered, CoffeeServed
from examples.coffee_bar.waiter.models.coffee import Coffee

waiter_bp = Blueprint('waiter')


@waiter_bp.route('/waiter/order-coffee', ['POST'])
async def coffee_new_order(request: Request) -> HTTPResponse:
    if request.json:
        # Creates coffee
        key_partitioner = KeyPartitioner()
        try:
            while True:
                coffee = Coffee(**request.json)
                if key_partitioner(coffee.uuid, [0, 1], [0, 1]) == request['waiter']['kafka_client'].cur_instance:
                    break
        except (KeyError, ValueError) as err:
            return response.json({'error': err, 'error_code': 500}, status=500)

        # Creates CoffeeOrdered event
        coffee_order = CoffeeOrdered(partition_key=coffee.uuid, **coffee.__to_dict__())

        # Send & await CoffeeOrdered
        event_record_metadata = await request['waiter']['producer'].send_and_wait(coffee_order, 'waiter-events')

        # Save coffee in own local store
        store_record_metedata = await request['waiter']['store_builder'].\
            set_from_local_store(coffee.uuid, coffee.__to_bytes_dict__())

        return response.json({'result': 'Coffee ordered', 'ticket': coffee.uuid, 'result_code': 200}, status=200)
    return response.json({'error': 'missing json', 'error_code': 500}, status=500)


@waiter_bp.route('/waiter/get-coffee/<uuid>', ['GET'])
async def coffee_get_order(request: Request, uuid: str) -> HTTPResponse:
    try:
        coffee = Coffee.__from_dict_bytes__(await request['waiter']['store_builder'].get_from_local_store(uuid))
    except StoreKeyNotFound as err:
        return response.json({'error': err, 'error_code': 500}, status=500)

    if coffee.state == 'awaiting':
        coffee_served = CoffeeServed(partition_key=coffee.uuid, uuid=coffee.uuid, served_to=coffee.coffee_for,
                                     is_payed=True, amount=coffee.amount, context=coffee.context)

        event_record_metadata = await request['waiter']['producer'].send_and_wait(coffee_served, 'waiter-events')

        coffee.set_state('served')

        store_record_metedata = await request['waiter']['store_builder'].\
            set_from_local_store(coffee.uuid, coffee.__to_bytes_dict__())

        return response.json({'result': 'Coffee served', 'ticket': coffee.uuid, 'result_code': 200}, status=200)
    elif coffee.state == 'served':
        return response.json({'result': 'Coffee already served', 'ticket': coffee.uuid, 'result_code': 500}, status=500)
    return response.json({'error': 'Unknown state', 'ticket': coffee.uuid, 'result_code': 500}, status=500)
