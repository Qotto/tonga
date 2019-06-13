#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains all command errors
"""

__all__ = [
    'CommandEventMissingProcessGuarantee'
]


class CommandEventMissingProcessGuarantee(Exception):
    """CommandEventMissingProcessGuarantee

    This error was raised if process_guarantee is not set on BaseCommand instantiation
    """
