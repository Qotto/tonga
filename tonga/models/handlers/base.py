#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" BaseHandler Class

All handlers must be inherit form this class
"""

__all__ = [
    'BaseHandler'
]


class BaseHandler:
    """ Base of all handler class
    """

    @classmethod
    def handler_name(cls) -> str:
        """ Return handler name, used by serializer

        Raises:
            NotImplementedError: Abstract def

        Returns:
            None
        """
        raise NotImplementedError
