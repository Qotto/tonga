#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

def my_exception_handler(loop, context):
    print('Exception handler called')

    pprint(context)
    print(type(context))
    print(context)
    if isinstance(context['exception'], ConsumerConnectionError):
        loop.stop()

waiter_app['loop'].set_exception_handler(my_exception_handler)


# -------------------------------------------------------------------------------


waiter_producer = asyncio.ensure_future(waiter_app['consumer'].listen_event('committed'),
                                        loop=waiter_app['loop'])

sanic_server = asyncio.ensure_future(server, loop=waiter_app['loop'])


async def start_app():
    res = await asyncio.gather(waiter_producer, sanic_server, return_exceptions=True)
    if isinstance(res[0], ConsumerConnectionError):
        print('ConsumerConnectionError')
        waiter_app['loop'].stop()
    return res


# Catch SIGINT
signal(SIGINT, lambda s, f: waiter_app['loop'].stop())
try:
    # Runs forever
    waiter_app['loop'].run_until_complete(start_app())
except Exception:
    # If an exception was raised loop was stopped
    waiter_app['loop'].stop()