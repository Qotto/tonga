#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from abc import ABCMeta, abstractmethod

from typing import Union, Dict, Any

from aiokafka import TopicPartition


class BasePositioning(metaclass=ABCMeta):
    _topic: str
    _partition: int
    _current_offset: Union[int, None]

    def get_topics(self) -> str:
        return self._topic

    def get_partition(self) -> int:
        return self._partition

    def get_current_offset(self) -> int:
        return self._current_offset

    def set_topics(self, topic) -> None:
        self._topic = topic

    def set_partition(self, partition) -> None:
        self._partition = partition

    def set_current_offset(self, current_offset: int) -> None:
        self._current_offset = current_offset

    @classmethod
    def make_class_assignment_key(cls, topic: str, partition: int) -> str:
        return topic + '-' + str(partition)

    def make_assignment_key(self) -> str:
        return self._topic + '-' + str(self._partition)

    @abstractmethod
    def to_topics_partition(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def from_topic_partition(cls, tp, offset):
        raise NotImplementedError

    def pprint(self) -> Dict[str, Any]:
        return {'topic': self._topic, 'partition': self._partition, 'offset': self._current_offset}


class KafkaPositioning(BasePositioning):

    def __init__(self, topic: str, partition: int, current_offset: int) -> None:
        self._topic = topic
        self._partition = partition
        self._current_offset = current_offset

    def to_topics_partition(self) -> TopicPartition:
        return TopicPartition(topic=self._topic, partition=self._partition)

    @classmethod
    def from_topic_partition(cls, tp: TopicPartition, offset: int):
        return cls(tp.topic, tp.partition, offset)
