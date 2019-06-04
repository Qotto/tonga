#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


class BaseHandler:
    @classmethod
    def handler_name(cls) -> str:
        raise NotImplementedError
