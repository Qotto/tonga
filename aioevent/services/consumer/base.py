#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from typing import List, Dict


__all__ = [
    'BaseConsumer',
]


class BaseConsumer:
    def __init__(self) -> None:
        pass

    async def start_consumer(self) -> None:
        raise NotImplementedError

    async def stop_consumer(self) -> None:
        raise NotImplementedError

    async def get_last_committed_offsets(self) -> Dict[int, int]:
        raise NotImplementedError

    async def get_current_offsets(self)-> Dict[int, int]:
        raise NotImplementedError

    async def get_beginning_offsets(self)-> Dict[int, int]:
        raise NotImplementedError

    async def get_last_offsets(self) -> Dict[int, int]:
        raise NotImplementedError

    async def listen_event(self, mod: str = 'latest') -> None:
        raise NotImplementedError

    async def get_many(self, partitions: List[int] = None, max_records: int = None):
        raise NotImplementedError

    async def get_one(self, partitions: List[int] = None):
        raise NotImplementedError

    async def seek_to_beginning(self, partition: int = None, topic: str = None) -> None:
        raise NotImplementedError

    async def seek_to_end(self, partition: int = None, topic: str = None) -> None:
        raise NotImplementedError

    async def seek_to_last_commit(self, partition: int = None, topic: str = None) -> None:
        raise NotImplementedError

    async def seek_custom(self, partition: int = None, topic: str = None,  offset: int = None) -> None:
        raise NotImplementedError

    async def subscriptions(self) -> frozenset:
        raise NotImplementedError
