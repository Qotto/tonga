#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contain BaseClient.

This class manage configuration on each Consumer / Producer / StoreManager
"""

from abc import ABCMeta

from tonga.services.coordinator.client.errors import (BadArgumentKafkaClient, CurrentInstanceOutOfRange)


__all__ = [
    'BaseClient'
]


class BaseClient(metaclass=ABCMeta):
    """ Class BaseClient

    Contain all repetitive information, this class was used in Consumer / Producer / StoreManager

    Attributes:
        cur_instance: (int): Current service instance
        nb_replica (int): Number of service replica
    """
    cur_instance: int
    nb_replica: int

    def __init__(self, cur_instance: int = 0, nb_replica: int = 1) -> None:
        """ BaseClient constructor

        Args:
            cur_instance: Current service instance
            nb_replica: Number of service replica

        Raises:
            BadArgumentKafkaClient: raised if argument are not valid
            CurrentInstanceOutOfRange: raised if current instance is highest than replica number
        """

        if isinstance(nb_replica, int) and isinstance(cur_instance, int):
            self.nb_replica = nb_replica
            self.cur_instance = cur_instance
        else:
            raise BadArgumentKafkaClient

        if self.cur_instance > self.nb_replica:
            raise CurrentInstanceOutOfRange
