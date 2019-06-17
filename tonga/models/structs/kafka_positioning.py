#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'KafkaPositioning'
]


class KafkaPositioning:
    """ KafkaPositioning class

    Class manage kafka positioning (like TopicsPartition)
    """
    topic: str
    partition: int

    def __int__(self, topic: str, partition: int, offset: int):
        self.topic = topic
        self.partition = partition
