#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse

from tests.coffee_bar.waiter.models.events import CoffeeOrdered, CoffeeServed
from tests.coffee_bar.waiter.models.coffee import Coffee

waiter_bp = Blueprint('waiter')


@waiter_bp.route('/waiter/order-coffee', ['POST'])
async def coffee_new_order(request: Request) -> HTTPResponse:
    if request.json:
        try:
            new_order = Coffee(**request.json)
        except (KeyError, ValueError) as err:
            return response.json({'error': err, 'error_code': 500}, status=500)
        coffee_order = CoffeeOrdered(partition_key=b'0', **new_order.__dict__)
        await request['aio_event'].producers['waiter_producer'].send_and_await(coffee_order, 'waiter-events')
        request['aio_event'].waiter_state.add_ordered_coffee(new_order)
        return response.json({'result': 'new coffee ordered', 'ticket': new_order.uuid, 'result_code': 201}, status=201)
    return response.json({'error': 'missing json', 'error_code': 500}, status=500)


@waiter_bp.route('/waiter/take-coffee/<ticket>', ['GET'])
async def take_coffee(request: Request, ticket: str) -> HTTPResponse:
    if ticket in request['aio_event'].waiter_state.awaiting_coffees:
        bundle_coffee = request['aio_event'].waiter_state.awaiting_coffees[ticket]
        coffee = bundle_coffee[0]
        context = bundle_coffee[1]
        served_coffee = CoffeeServed(partition_key=b'1', uuid=coffee.uuid, served_to=coffee.served_to, is_payed=True,
                                     amount=context['amount'], context=context)
        await request['aio_event'].producers['waiter_producer'].send_and_await(served_coffee, 'waiter-events')
        request['aio_event'].waiter_state.served_coffee(coffee.uuid)
        return response.json({'result': 'Take your coffee', 'ticket': coffee.uuid, 'result_code': 200}, status=200)
    elif ticket in request['aio_event'].waiter_state.served_coffees:
        return response.json({'result': 'Coffee Already Served', 'ticket': ticket, 'result_code': 200}, status=200)
    return response.json({'error': 'Coffee now found', 'ticket': ticket, 'result_code': 200}, status=404)
