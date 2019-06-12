#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" Contains all statefulset_assignors errors
"""

__all__ = [
    'BadAssignorPolicy',
]


class BadAssignorPolicy(ValueError):
    """BadAssignorPolicy

    This error was raised when assignor policy not matched
    """
