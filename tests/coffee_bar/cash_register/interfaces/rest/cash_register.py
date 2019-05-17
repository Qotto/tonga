#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse


cash_register_bp = Blueprint('Cash Register')


@cash_register_bp.route('/cash-register', ['GET'])
async def cash_register(request: Request) -> HTTPResponse:
    in_cash, out_cash = request['aio_event'].get('cash_register_repository').how_mush_in_and_out()
    return response.json({
        'amount_in': in_cash,
        'amount_awaiting': out_cash
    }, status=200)


@cash_register_bp.route('/cash-register/full', ['GET'])
async def cash_register(request: Request) -> HTTPResponse:
    db = request['aio_event'].get('cash_register_repository').get_db()
    l = list()
    for uuid, bill in db.items():
        l.append(bill.__dict__)
    return response.json(l, status=200)
