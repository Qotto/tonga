#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import asyncio
import logging
from aioevent import AioEvent
from aioevent.model import BaseCommand
from aiokafka import TopicPartition, AIOKafkaProducer

from typing import Dict, Any

from ..events import CoffeeStarted
from ..results import MakeCoffeeResult

__all__ = [
    'MakeCoffee'
]


async def make_transaction(serializer, transactional_id, topic, offset, uuid, context, group_id):
    loop = asyncio.get_event_loop()

    transactional_producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092',
                                              client_id='coffee_maker_transactional',
                                              value_serializer=serializer.encode,
                                              transactional_id=transactional_id)

    print('Transactional Producer before start')

    await transactional_producer.start()

    print('Transactional Producer Started')

    max_retries = 5

    retries = 0
    # Tries and retries transaction
    while True:
        print('IN WHILE')
        try:
            # Start transaction
            print('BEFORE TRA')
            async with transactional_producer.transaction():
                # Create commit offsets (used in end of transaction)
                print('IN ASYNC')
                commit_offsets = dict()
                commit_offsets[topic] = offset + 1

                # Create event
                coffee_started = CoffeeStarted(uuid, coffee_for=context['coffee_for'],
                                               context=context)
                make_coffee_result = MakeCoffeeResult(uuid, context=context)

                # Send and wait event & result
                print('SEND')
                await transactional_producer.send_and_wait(coffee_started, 'coffee-maker-events')
                await transactional_producer.send_and_wait(make_coffee_result, 'coffee-maker-results')

                # Commit transaction
                await transactional_producer.send_offsets_to_transaction(commit_offsets, group_id)
        except Exception as e:
            print('Transactional exception = ', e)
            if retries >= max_retries:
                break
            retries += 1
        else:
            print('BREAK')
            await transactional_producer.stop()
            break
        print('END WHILE')
    print('END')


def init_transaction(loop, serializer, transactional_id, topic, offset, uuid, context, group_id):
    print(loop)
    asyncio.set_event_loop(loop)
    loop.run_until_complete(make_transaction(serializer, transactional_id, topic, offset, uuid, context, group_id))


class MakeCoffee(BaseCommand):
    uuid: str
    cup_type: str
    coffee_type: str

    def __init__(self, uuid: str, cup_type: str, coffee_type: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type

    async def execute(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)

        loop = asyncio.get_event_loop()

        transactional_producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092',
                                                  client_id='coffee_maker_transactional',
                                                  value_serializer=app.serializer.encode,
                                                  transactional_id=self.record_id)

        print('Transactional Producer before start')

        await transactional_producer.start()

        print('Transactional Producer Started')

        max_retries = 5

        retries = 0
        # Tries and retries transaction
        while True:
            print('IN WHILE')
            try:
                # Start transaction
                print('BEFORE TRA')
                async with transactional_producer.transaction():
                    # Create commit offsets (used in end of transaction)
                    print('IN ASYNC')
                    commit_offsets = dict()
                    commit_offsets[topic] = offset + 1

                    # Create event
                    coffee_started = CoffeeStarted(self.uuid, coffee_for=self.context['coffee_for'],
                                                   context=self.context)
                    make_coffee_result = MakeCoffeeResult(self.uuid, context=self.context)

                    # Send and wait event & result
                    print('SEND')
                    await transactional_producer.send_and_wait(coffee_started, 'coffee-maker-events')
                    await transactional_producer.send_and_wait(make_coffee_result, 'coffee-maker-results')

                    # Commit transaction
                    await transactional_producer.send_offsets_to_transaction(commit_offsets, group_id)
            except Exception as e:
                print('Transactional exception = ', e)
                if retries >= max_retries:
                    break
                retries += 1
            else:
                print('BREAK')
                await transactional_producer.stop()
                break
            print('END WHILE')
        print('END')

        return 'transaction'

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.command.MakeCoffee'
