#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'BadKeyType',
    'OutsideInstanceNumber'
]

# ---------- Start KeyPartitioner Exceptions ----------


class BadKeyType(TypeError):
    pass

# ---------- End KeyPartitioner Exceptions ----------

# ---------- Start StatefulsetPartitioner Exceptions ----------


class OutsideInstanceNumber(SystemError):
    pass

# ---------- End StatefulsetPartitioner Exceptions ----------
