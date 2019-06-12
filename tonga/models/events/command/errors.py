#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'CommandEventMissingProcessGuarantee'
]


class CommandEventMissingProcessGuarantee(Exception):
    """CommandEventMissingProcessGuarantee

    This error was raised if process_guarantee is not set on BaseCommand instantiation
    """
