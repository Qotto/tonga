#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'AioEventBaseException',
]


class AioEventBaseException(Exception):
    """
    Base exceptions
    Args:
        msg (str): Human readable string describing the exception.
        code (:obj:`int`, optional): Error code.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
    """
    def __init__(self, msg, code):
        self.msg = msg
        self.code = code
