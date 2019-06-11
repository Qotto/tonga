#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'BadKeyType',
    'OutsideInstanceNumber'
]


class BadKeyType(TypeError):
    """BadKeyType

    This error was raised in KeyPartitioner when key was not bytes
    """


class OutsideInstanceNumber(SystemError):
    """OutsideInstanceNumber

    This error was raised in StateFulSetPartitioner when instance number is out of range
    """
