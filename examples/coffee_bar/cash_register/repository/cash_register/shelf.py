#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import shelve
from shelve import DbfilenameShelf

from typing import List, Tuple

from .base import BaseCashRegisterRepository
from ..base import NotFound
from ...models.bill import Bill


class ShelfCashRegisterRepository(BaseCashRegisterRepository):
    _db_path: str
    _db: DbfilenameShelf

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._db = shelve.open(self._db_path)

    def add_bill(self, obj: Bill) -> None:
        self._db[obj.uuid] = obj

    def upd_bill(self, obj: Bill) -> None:
        if obj.uuid not in self._db:
            raise NotFound
        self._db[obj.uuid] = obj

    def get_bill_by_uuid(self, uuid: str) -> Bill:
        if uuid not in self._db:
            raise NotFound
        return self._db[uuid]

    def get_bill_list(self, unpaid: bool, paid: bool) -> List[Bill]:
        bills = list()

        for uuid, bill in self._db.items():
            if unpaid:
                if not bill.is_paid:
                    bills.append(bill)
                    continue
            elif paid:
                if bill.is_paid:
                    bills.append(bill)
                    continue

        return bills

    def remove_bill(self, obj: Bill) -> None:
        if obj.uuid not in self._db:
            raise NotFound
        del self._db[obj.uuid]

    def how_mush_in_and_out(self) -> Tuple[float, float]:
        in_cash: float = 0
        out_cash: float = 0

        for uuid, bill in self._db.items():
            if not bill.is_paid:
                out_cash += bill.amount
            elif bill.is_paid:
                in_cash += bill.amount
        return in_cash, out_cash

    def get_db(self):
        return self._db
