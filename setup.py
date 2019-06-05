#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

from setuptools import setup

setup(
    name='tonga',
    version='0.0.1',
    packages=['tonga'],
    url='https://github.com/qotto/tonga',
    license='MIT',
    author='Qotto',
    author_email='contact@qotto.net',
    include_package_data=True,
    description='Client for build event driven app with Apache Kafka distributed stream processing system with asyncio',
    install_requires=[
        'avro-python3==1.9.0',
        'pyyaml==5.1',
        'aiokafka==0.5.1',
        'kafka-python==1.4.6'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
        'Topic :: System :: Networking',
        'Topic :: System :: Distributed Computing',
        'Topic :: Database',
        'Framework :: AsyncIO',
        'Development Status :: 2 - Pre-Alpha',
    ],
)
