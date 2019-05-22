#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


class BaseApp:
    def attach(self, object_name: str, object_instance: object):
        raise NotImplementedError

    def get(self, object_name: str) -> object:
        raise NotImplementedError
