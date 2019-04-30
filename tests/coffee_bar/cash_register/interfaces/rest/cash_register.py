#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse


cash_register_bp = Blueprint('Cash Register')


@cash_register_bp.route('/cash-register', ['GET'])
async def cash_register(request: Request) -> HTTPResponse:
    return response.json({
        'amount_in': request['aio_event'].cash_register_state.how_mush_paid(),
        'amount_awaiting': request['aio_event'].cash_register_state.how_mush_awaiting()
    }, status=200)
