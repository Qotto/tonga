import uvloop
import asyncio
import json
from datetime import datetime, timezone
from signal import signal, SIGINT

# IN INIT /!\
kafka_client: BaseClient: ...
transactional_streams: BaseTransaction ...
# IN INIT /!\

class Bartender:
    streams: BaseStream
    store_manager: BaseStoreManager

    def __init__(self, streams: BaseStreams, store_manager: BaseStoreManager) -> None:
        self.streams = streams
        self.store_manager = store_manager

    async def handle_coffee_asked(self, event: BillCreated) -> None:
        context = event.context
        context['bartender.finished'] = False
        context['bartender.start_time'] = datetime.now(timezone.utc).isoformat()

        # Do stuff
        coffee_machine_command = CoffeeMachineCommand(event.coffee_uuid, event.coffee_type, context=context)

        self.store_manager.set_entry_in_local_store(event.coffee_uuid, json.dumps({
            'coffee_for': event.coffee_for,
            'coffee_type': event.coffee_type,
            'context': event.context
        }))

        await self.streams['coffee-machine-commands'].publish(coffee_machine_command)


    @transactional_streams
    async def handle_coffee_machine_result(self, result: CoffeeMachineResult) -> None:
        context = result.context

        context['bartender.finished'] = True
        context['bartender.end_time'] = datetime.now(timezone.utc).isoformat()
        context['bartender.delta_time'] = datetime.fromtimestamp(context['bartender.end_time'], timezone.utc) - \
                                          datetime.fromtimestamp(context['bartender.start_time'], timezone.utc)

        # Do stuff
        coffee_finished = CoffeeFinished(result.coffee_uuid, result.coffee_for, context)

        await transactional_streams['bartender_events'].publish(coffee_finished)

        # Store part
        coffee_record = json.loads(await self.store_manager.get_entry_in_local_store(result.coffee_uuid))

        coffee_record['context'] = context

        await self.store_manager.set_entry_in_local_store(result.coffee_uuid, json.dumps(coffee_record))

    async def consume_bills(self) -> None:
        self.streams.start()
        async for bills in self.streams['cash-register-events']:
            await self.handle_coffee_asked(bills)
            await self.streams.commit()

    async def consume_coffee_machine_result(self) -> None:
        self.streams.start()
        async for coffee_machine_result in self.streams['coffee-machine-results']:
            await self.handle_coffee_machine_result(coffee_machine_result)
            await self.streams.commit()

    def __del__(self) -> None:
        self.streams.stop()


if __name__ == '__main__':
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    loop = asyncio.get_event_loop()

    # /!\ Streams & BaseStoreManager use BaseClient form init Pyfile /!\
    streams: BaseStream: ...
    store_manager: BaseStoreManager: ...

    bartender = Bartender(streams, store_manager)

    asyncio.ensure_future(bartender.consume_bills(), loop=loop)

    asyncio.ensure_future(bartender.consume_coffee_machine_result(), loop=loop)

    signal(SIGINT, lambda s, f: loop.stop())
    try:
        # Runs forever
        loop.run_forever()
    except Exception:
        # If an exception was raised loop was stopped
        loop.stop()
