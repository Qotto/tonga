#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
from colorlog import ColoredFormatter


def setup_logger():
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s]%(levelname)s: %(name)s/%(module)s/%(funcName)s:%(lineno)d"
        " (%(thread)d) %(blue)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )

    logger = logging.getLogger('tonga')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger
