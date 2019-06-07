#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


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
