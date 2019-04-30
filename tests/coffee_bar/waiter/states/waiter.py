#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from typing import Dict, List, Any, Union

from ..models.coffee import Coffee


class WaiterState(object):
    ordered_coffees: Dict[str, Coffee]
    served_coffees: Dict[str, List[Union[Coffee, Dict[str, Any]]]]
    awaiting_coffees: Dict[str, List[Union[Coffee, Dict[str, Any]]]]

    state_served_coffees: List[str]

    def __init__(self):
        self.ordered_coffees = dict()
        self.served_coffees = dict()
        self.awaiting_coffees = dict()

        self.state_served_coffees = list()

    def add_ordered_coffee(self, coffee: Coffee) -> None:
        self.ordered_coffees[coffee.uuid] = coffee

    def coffee_awaiting(self, uuid: str, context: Dict[str, Any]) -> None:
        self.awaiting_coffees[uuid][0] = self.ordered_coffees[uuid]
        self.awaiting_coffees[uuid][1] = context
        del self.ordered_coffees[uuid]

    def coffee_served(self, uuid: str) -> None:
        self.served_coffees[uuid] = self.awaiting_coffees[uuid]
        del self.awaiting_coffees[uuid]

    def add_coffee_served_state(self, uuid) -> None:
        self.state_served_coffees.append(uuid)

    def init_state(self) -> None:
        for uuid, value in self.ordered_coffees.items():
            if uuid in self.awaiting_coffees:
                if uuid in self.state_served_coffees:
                    self.served_coffees[uuid] = self.awaiting_coffees[uuid]
                    del self.awaiting_coffees[uuid]
        del self.state_served_coffees
