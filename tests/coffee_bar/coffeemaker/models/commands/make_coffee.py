#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from aioevent import AioEvent
from aioevent.model import BaseCommand
from aioevent.model.exceptions import KafkaProducerError, KafkaConsumerError
from aiokafka import TopicPartition

from typing import Dict, Any, Union

from ..events import CoffeeStarted
from ..results import MakeCoffeeResult

__all__ = [
    'MakeCoffee'
]


class MakeCoffee(BaseCommand):
    uuid: str
    cup_type: str
    coffee_type: str

    def __init__(self, uuid: str, cup_type: str, coffee_type: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid
        self.cup_type = cup_type
        self.coffee_type = coffee_type

    async def execute(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition, offset: int) -> \
            Union[str, None]:
        if not app.producers['coffee_maker_transactional_producer'].running:
            await app.producers['coffee_maker_transactional_producer'].start_producer()

        max_retries = 5
        retries = 0
        # Tries and retries transaction
        while True:
            try:
                # Starts transaction
                async with app.producers['coffee_maker_transactional_producer'].kafka_producer.transaction():
                    # Creates committed offsets
                    commit_offsets = dict()
                    commit_offsets[topic] = offset + 1

                    # Creates CoffeeStarted event and MakeCoffeeResult result
                    coffee_started = CoffeeStarted(self.uuid, context=self.context)
                    make_coffee_result = MakeCoffeeResult(self.uuid, context=self.context)

                    # Sends and awaits event & result
                    await app.producers['coffee_maker_transactional_producer'].send(coffee_started,
                                                                                    'coffee-maker-events')
                    await app.producers['coffee_maker_transactional_producer'].send(make_coffee_result,
                                                                                    'coffee-maker-results')

                    # Test transaction fail big crash
                    # exit(-1)

                    # Commits transaction
                    print('Transaction commit : ', commit_offsets)
                    await app.producers['coffee_maker_transactional_producer'].\
                        kafka_producer.send_offsets_to_transaction(commit_offsets, group_id)
            except (KafkaProducerError, Exception) as e:
                if retries >= max_retries:
                    raise KafkaConsumerError(f'Error info : {e}', 500)
                retries += 1
            else:
                break
        return 'transaction'

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.command.MakeCoffee'
