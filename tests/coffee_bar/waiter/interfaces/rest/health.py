#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse


health_bp = Blueprint('health')


@health_bp.route('/health', ['GET'])
async def health_check(request: Request) -> HTTPResponse:
    return response.text('ok', 200)
