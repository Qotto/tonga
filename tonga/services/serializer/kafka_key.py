#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" KafkaKeySerializer

Serialize string key to bytes & deserialize key in string
"""

from tonga.services.serializer.base import BaseSerializer
from tonga.services.serializer.errors import KeySerializerDecodeError, KeySerializerEncodeError


class KafkaKeySerializer(BaseSerializer):
    """ Serialize kafka key to bytes
    """

    @classmethod
    def encode(cls, obj: str) -> bytes:
        """ Encode key to bytes for kafka

        Args:
            obj (str): Key in string or bytes format

        Raises:
            KeySerializerEncodeError: this error was raised when KafkaKeySerializer can't serialize key

        Returns:
            bytes: Kafka key as bytes
        """
        if isinstance(obj, str) and obj is not None:
            return obj.encode('utf-8')
        if isinstance(obj, bytes):
            return obj
        raise KeySerializerEncodeError

    @classmethod
    def decode(cls, encoded_obj: bytes) -> str:
        """ Decode kafka key to str

        Args:
            encoded_obj (bytes): Kafka key in bytes

        Raises:
            KeySerializerDecodeError: this error was raised when KafkaKeySerializer can't deserialize key

        Returns:
            str: Key as string
        """
        if isinstance(encoded_obj, bytes) and encoded_obj is not None:
            return encoded_obj.decode('utf-8')
        if isinstance(encoded_obj, str):
            return encoded_obj
        raise KeySerializerDecodeError
