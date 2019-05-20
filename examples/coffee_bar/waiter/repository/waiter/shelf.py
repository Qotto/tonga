#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import shelve
from shelve import DbfilenameShelf

from typing import List

from .base import BaseWaiterRepository
from ..base import NotFound
from ...models.coffee import Coffee


class ShelfWaiterRepository(BaseWaiterRepository):
    _db_path: str
    _db: DbfilenameShelf

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._db = shelve.open(self._db_path)

    def add_coffee(self, obj: Coffee) -> None:
        self._db[obj.uuid] = obj

    def upd_coffee(self, obj: Coffee) -> None:
        self._db[obj.uuid] = obj

    def get_coffee_by_uuid(self, uuid: str) -> Coffee:
        if uuid not in self._db:
            raise NotFound
        return self._db[uuid]

    def coffee_is_ready(self, uuid: str) -> bool:
        if uuid not in self._db:
            return False
        if self._db[uuid].state == 'awaiting':
            return True
        return False

    def get_coffee_list(self, ordered: bool = False, awaiting: bool = False, served: bool = False) -> List[Coffee]:
        coffees = list()

        for uuid, coffee in self._db.items():
            if ordered:
                if coffee.state == 'ordered':
                    coffees.append(coffee)
                    continue
            elif awaiting:
                if coffee.state == 'awaiting':
                    coffees.append(coffee)
                    continue
            elif served:
                if coffee.state == 'served':
                    coffees.append(coffee)
                    continue

        return coffees

    def remove_coffee(self, obj: Coffee) -> None:
        if obj.uuid not in self._db:
            raise NotFound
        del self._db[obj.uuid]
