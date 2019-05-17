import asyncio
import time
from aiohttp import ClientSession


async def order_coffee(url, session, payload):
    async with session.post(url, json=payload) as response:
        return await response.json()


async def take_coffee(url, session):
    served = False
    result = {}
    while not served:
        async with session.get(url) as response:
            result = await response.json()
        if result['result_code'] == 200:
            served = True
        else:
            await asyncio.sleep(1)
    return result


async def run(r):
    url_order_coffee = "http://localhost:8000/waiter/order-coffee"
    url_take_coffee = 'http://localhost:8000/waiter/take-coffee/'
    payload = {
        'coffee_type': 'Classic',
        'coffee_for': 'Theo',
        'cup_type': 'Venti',
        'amount': 2.5
    }
    order_tasks = []
    take_tasks = []

    async with ClientSession() as session:
        # Orders coffees
        for i in range(r):
            task = asyncio.ensure_future(order_coffee(url_order_coffee, session, payload))
            order_tasks.append(task)
        ordered_responses = await asyncio.gather(*order_tasks)
        print(ordered_responses)

        # Takes coffees
        for i in range(r):
            uuid = ordered_responses[i]['ticket']
            task = asyncio.ensure_future(take_coffee((url_take_coffee + uuid), session))
            take_tasks.append(task)
        take_responses = await asyncio.gather(*take_tasks)
        print(take_responses)

start = time.time()
loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(1000))
loop.run_until_complete(future)
end = time.time()
print(end - start)
