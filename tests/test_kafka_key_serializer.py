#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import pytest


from aioevent.services.serializer.kafka_key import KafkaKeySerializer


def test_kafka_key_serializer_encode_string():
    assert KafkaKeySerializer.encode('test') == b'test'


def test_kafka_key_serializer_encode_bytes():
    assert KafkaKeySerializer.encode(b'test') == b'test'


def test_kafka_key_serializer_encode_bad_format():
    with pytest.raises(ValueError):
        assert KafkaKeySerializer.encode(['test'])


def test_kafka_key_serializer_decode_bytes():
    assert KafkaKeySerializer.decode(b'test') == 'test'


def test_kafka_key_serializer_decode_string():
    assert KafkaKeySerializer.decode('test') == 'test'


def test_kafka_key_serializer_decode_bad_format():
    with pytest.raises(ValueError):
        assert KafkaKeySerializer.decode(['test'])
