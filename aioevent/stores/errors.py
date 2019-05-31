#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


__all__ = [
    'StoreKeyNotFound',
    'StoreMetadataCantNotUpdated',
]


class StoreKeyNotFound(Exception):
    pass


class StoreMetadataCantNotUpdated(Exception):
    pass
