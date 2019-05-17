#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import asyncio
from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse

from tests.coffee_bar.waiter.models.events import CoffeeOrdered, CoffeeServed
from tests.coffee_bar.waiter.models.coffee import Coffee

waiter_bp = Blueprint('waiter')


@waiter_bp.route('/waiter/order-coffee', ['POST'])
async def coffee_new_order(request: Request) -> HTTPResponse:
    if request.json:
        # Create coffee
        try:
            coffee = Coffee(**request.json)
        except (KeyError, ValueError) as err:
            return response.json({'error': err, 'error_code': 500}, status=500)
        # Create CoffeeOrdered event
        coffee_order = CoffeeOrdered(partition_key=bytes(str(request['aio_event'].instance), 'utf-8'),
                                     **coffee.__to_event_dict__())
        # Send & await CoffeeOrdered
        await request['aio_event'].producers['waiter_producer'].send_and_await(coffee_order, 'waiter-events')

        # Check if coffee is ready to serves
        while not request['aio_event'].get('waiter_local_repository').coffee_is_ready(coffee.uuid):
            await asyncio.sleep(1)

        # Get new coffee in local repository
        coffee = request['aio_event'].get('waiter_local_repository').get_coffee_by_uuid(coffee.uuid)

        # Create CoffeeServed event
        served_coffee = CoffeeServed(partition_key=bytes(str(request['aio_event'].instance), 'utf-8'), uuid=coffee.uuid,
                                     served_to=coffee.coffee_for, is_payed=True, amount=coffee.amount,
                                     context=coffee.context)

        # Send CoffeeServed event
        await request['aio_event'].producers['waiter_producer'].send_and_await(served_coffee, 'waiter-events')

        return response.json({'result': 'Take your coffee', 'ticket': coffee.uuid, 'result_code': 200}, status=200)
    return response.json({'error': 'missing json', 'error_code': 500}, status=500)
