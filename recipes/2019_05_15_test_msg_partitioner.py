#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from kafka.partitioner.hashed import murmur2
from uuid import uuid4
import random


def partioner(key, all_partitions, available):
    idx = (murmur2(key) & 0x7fffffff) % len(all_partitions)
    return idx


all_parts = [0, 1, 2, 3, 4, 5]
available_parts = [0, 1, 2, 3, 4, 5]
p0 = 0
p1 = 0
p2 = 0
p3 = 0
p4 = 0
p5 = 0
for i in range(0, 10000):
    key = bytes(uuid4().hex, 'utf-8')
    part = partioner(key, all_parts, available_parts)
    if part == 0:
        p0 += 1
    elif part == 1:
        p1 += 1
    elif part == 2:
        p2 += 1
    elif part == 3:
        p3 += 1
    elif part == 4:
        p4 += 1
    elif part == 5:
        p5 += 1

print(f'p0: {p0}\np1: {p1}\np2: {p2}\np3: {p3}\np4: {p4}\np5: {p5}\n')
