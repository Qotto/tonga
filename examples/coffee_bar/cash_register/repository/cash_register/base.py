#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import List

from ..base import BaseRepository
from ...models.bill import Bill


class BaseCashRegisterRepository(BaseRepository):
    def add_bill(self, obj: Bill) -> None:
        raise NotImplementedError

    def upd_bill(self, obj: Bill) -> None:
        raise NotImplementedError

    def get_bill_by_uuid(self, uuid: str) -> Bill:
        raise NotImplementedError

    def get_bill_list(self, unpaid: bool, paid: bool) -> List[Bill]:
        raise NotImplementedError

    def remove_bill(self, obj: Bill) -> None:
        raise NotImplementedError
