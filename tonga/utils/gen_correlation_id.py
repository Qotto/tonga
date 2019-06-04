#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from base64 import b64encode
from datetime import datetime, timezone
from secrets import token_urlsafe

__all__ = [
    'gen_correlation_id',
]


def gen_correlation_id(prefix: str = None) -> str:
    """
    Generates a correlation ID that looks like `prefix:date:random`, where:
    - `prefix` is a fixed part that you can specify (any length)
    - `date` only depends on the current date (8 characters)
    - `random` is a random part  (4 characters)
    `date` and `random` are encoded as `[a-zA-Z0-9_-]`.
    """
    CORRELATION_ID_PREFIX_LENGTH = 6
    CORRELATION_ID_TOKEN_LENGTH = 3

    def ts2000res65536() -> bytes:
        """
        Converts current date to 6 bytes
        """
        ts_now = datetime.now(timezone.utc).timestamp()
        ts_2k = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp()
        return int(65536 * (ts_now - ts_2k)).to_bytes(6, 'big')

    if prefix is None:
        prefix = token_urlsafe(CORRELATION_ID_PREFIX_LENGTH)
    date = b64encode(ts2000res65536(), b'_-').decode('ascii')
    random = token_urlsafe(CORRELATION_ID_TOKEN_LENGTH)
    return f'{prefix}:{date}:{random}'
