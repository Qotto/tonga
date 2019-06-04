#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.errors import KeySerializerDecodeError, KeySerializerEncodeError


class KafkaKeySerializer(BaseSerializer):
    @classmethod
    def encode(cls, key: str) -> bytes:
        if isinstance(key, str) and key is not None:
            return key.encode('utf-8')
        if isinstance(key, bytes):
            return key
        raise KeySerializerEncodeError

    @classmethod
    def decode(cls, key: bytes) -> str:
        if isinstance(key, bytes) and key is not None:
            return key.decode('utf-8')
        if isinstance(key, str):
            return key
        raise KeySerializerDecodeError
