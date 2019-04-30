#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aioevent import BaseResult
from aioevent.app.base import BaseApp
from aiokafka import TopicPartition

from typing import Dict, Any

from ..events import CoffeeFinished

__all__ = [
    'MakeCoffeeResult'
]


class MakeCoffeeResult(BaseResult):
    uuid: str

    def __init__(self, uuid: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid

    async def on_result(self, app: BaseApp, corr_id: str, group_id: str, topic: TopicPartition, offset: int):
        print(self.__dict__)

        # Create transactional producer
        transactional_producer = await app.producers['coffee_maker_producer'].get_transactional_producer(self.record_id)

        # Check error
        if self.error is None:
            # Do stuff if an error as been reached
            return

        max_retries = 5
        retries = 0
        # Tries and retries transaction
        while True:
            try:
                # Start transaction
                async with transactional_producer.transaction():
                    # Create commit offsets (used in end of transaction)
                    commit_offsets = dict()
                    commit_offsets[topic] = offset + 1

                    # Create event
                    context = self.context
                    context['coffee_finished'] = datetime.now(timezone.utc).timestamp()
                    delta_coffee_time = (context['coffee_stated'] - context['coffee_finished']).total_seconds()
                    coffee_finished = CoffeeFinished(self.uuid, cup_type=self.context['cup_type'],
                                                     coffee_type=self.context['coffee_type'],
                                                     coffee_for=self.context['coffee_for'],
                                                     coffee_time=delta_coffee_time, context=context)
                    # Send and wait event & result
                    await app.producers['bartender_event_producer'].send_and_wait(coffee_finished, 'bartender-events')

                    # Commit transaction
                    await transactional_producer.send_offsets_to_transaction(commit_offsets, group_id)
            except Exception as e:
                if retries >= max_retries:
                    break
                retries += 1
            else:
                await transactional_producer.stop()
                break

    @classmethod
    def from_data(cls, event_data: Dict[str, Any]):
        return cls(**event_data)

    @classmethod
    def event_name(cls) -> str:
        return 'aioevent.coffeemaker.result.MakeCoffeeResult'
