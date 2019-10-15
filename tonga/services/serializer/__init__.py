#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Regular packages

Manage Serializer package
"""

from .base import BaseSerializer
from .avro import AvroSerializer

__all__ = [
    'BaseSerializer',
    'AvroSerializer',
]
