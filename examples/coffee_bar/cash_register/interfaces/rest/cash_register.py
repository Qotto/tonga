#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse

from examples.coffee_bar.cash_register.models.bill import Bill

cash_register_bp = Blueprint('Cash Register')


@cash_register_bp.route('/cash-register', ['GET'])
async def cash_register(request: Request) -> HTTPResponse:
    local_store = request['cash_register']['store_builder'].get_local_store().get_all()
    global_store = request['cash_register']['store_builder'].get_global_store().get_all()

    local_cash_in = 0
    local_cash_out = 0
    for key, value in local_store.items():
        bill = Bill.__from_bytes_dict__(value)
        if bill.is_paid:
            local_cash_in += bill.amount
        else:
            local_cash_out += bill.amount

    global_cash_in = 0
    global_cash_out = 0
    for key, value in global_store.items():
        bill = Bill.__from_bytes_dict__(value)
        if bill.is_paid:
            global_cash_in += bill.amount
        else:
            global_cash_out += bill.amount

    return response.json({
        'local_cash_in': local_cash_in,
        'local_cash_out': local_cash_out,
        'global_cash_in': global_cash_in,
        'global_cash_out': global_cash_out
    }, status=200)
