#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.errors import KeySerializerDecodeError, KeySerializerEncodeError


class KafkaKeySerializer(BaseSerializer):
    """ Serialize kafka key to bytes
    """

    @classmethod
    def encode(cls, key: str) -> bytes:
        """ Encode key to bytes for kafka

        Args:
            key (str): Key in string or bytes format

        Raises:
            KeySerializerEncodeError: this error was raised when KafkaKeySerializer can't serialize key

        Returns:
            bytes: Kafka key as bytes
        """
        if isinstance(key, str) and key is not None:
            return key.encode('utf-8')
        if isinstance(key, bytes):
            return key
        raise KeySerializerEncodeError

    @classmethod
    def decode(cls, key: bytes) -> str:
        """ Decode kafka key to str

        Args:
            key (bytes): Kafka key in bytes

        Raises:
            KeySerializerDecodeError: this error was raised when KafkaKeySerializer can't deserialize key

        Returns:
            str: Key as string
        """
        if isinstance(key, bytes) and key is not None:
            return key.decode('utf-8')
        if isinstance(key, str):
            return key
        raise KeySerializerDecodeError
