#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import List

from ..base import BaseRepository
from ...models.coffee import Coffee


class BaseWaiterRepository(BaseRepository):
    def add_coffee(self, obj: Coffee) -> None:
        raise NotImplementedError

    def upd_coffee(self, obj: Coffee) -> None:
        raise NotImplementedError

    def get_coffee_by_uuid(self, uuid: str) -> Coffee:
        raise NotImplementedError

    def get_coffee_list(self, ordered: bool, awaiting: bool, served: str) -> List[Coffee]:
        raise NotImplementedError

    def remove_coffee(self, obj: Coffee) -> None:
        raise NotImplementedError
