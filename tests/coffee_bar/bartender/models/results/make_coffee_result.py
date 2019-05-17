#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from datetime import datetime, timezone
from aioevent import BaseResult, AioEvent
from aioevent.model.exceptions import KafkaProducerError, KafkaConsumerError
from aiokafka import TopicPartition

from typing import Dict, Any, Union

from ..events import CoffeeFinished

__all__ = [
    'MakeCoffeeResult'
]


class MakeCoffeeResult(BaseResult):
    uuid: str

    def __init__(self, uuid: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.uuid = uuid

    async def on_result(self, app: AioEvent, corr_id: str, group_id: str, topic: TopicPartition,
                        offset: int) -> Union[str, None]:
        if not app.producers['bartender_transactional_producer'].running:
            await app.producers['bartender_transactional_producer'].start_producer()

        # Checks error
        if self.error is not None:
            # Do stuff if an error as been reached
            return 'error'

        max_retries = 5
        retries = 0
        # Tries and retries transaction
        while True:
            try:
                # Starts transaction
                async with app.producers['bartender_transactional_producer'].kafka_producer.transaction():
                    # Creates committed offsets
                    commit_offsets = dict()
                    commit_offsets[topic] = offset + 1

                    # Creates CoffeeFinished event
                    context = self.context
                    context['coffee_finished'] = datetime.now(timezone.utc).timestamp()
                    delta_coffee_time = (datetime.fromtimestamp(context['start_time'], timezone.utc) -
                                         datetime.fromtimestamp(context['coffee_finished'], timezone.utc)).\
                        total_seconds()

                    coffee_finished = CoffeeFinished(self.uuid, coffee_for=self.context['coffee_for'],
                                                     coffee_time=delta_coffee_time, context=context)
                    # Sends and awaits event
                    await app.producers['bartender_transactional_producer'].send(coffee_finished, 'bartender-events')

                    # Commits transaction
                    print('Transaction commit : ', commit_offsets)
                    await app.producers['bartender_transactional_producer'].kafka_producer.send_offsets_to_transaction(
                        commit_offsets, group_id)
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
        return 'aioevent.coffeemaker.result.MakeCoffeeResult'
