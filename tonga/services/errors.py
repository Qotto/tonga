#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains common used by producer & consumer
"""

__all__ = [
    'BadSerializer'
]


class BadSerializer(TypeError):
    """BadSerializer

    This error was raised when consumer or producer receive a bad serializer instance
    """
