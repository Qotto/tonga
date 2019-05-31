#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019


from setuptools import setup

setup(
    name='aioevent',
    version='0.0.1',
    packages=['tests', 'tests.misc', 'tests.misc.event_class', 'tests.misc.handler_class', 'aioevent', 'aioevent.utils',
              'aioevent.models', 'aioevent.models.events', 'aioevent.models.events.event',
              'aioevent.models.events.result', 'aioevent.models.events.command', 'aioevent.models.store_record',
              'aioevent.stores', 'aioevent.stores.local', 'aioevent.stores.globall', 'aioevent.stores.store_builder',
              'aioevent.services', 'aioevent.services.consumer', 'aioevent.services.producer',
              'aioevent.services.serializer', 'aioevent.services.coordinator',
              'aioevent.services.coordinator.assignors', 'aioevent.services.coordinator.partitioner', 'examples',
              'examples.coffee_bar', 'examples.coffee_bar.waiter', 'examples.coffee_bar.waiter.models',
              'examples.coffee_bar.waiter.models.events', 'examples.coffee_bar.waiter.models.handlers',
              'examples.coffee_bar.waiter.interfaces', 'examples.coffee_bar.waiter.interfaces.rest',
              'examples.coffee_bar.bartender', 'examples.coffee_bar.bartender.models',
              'examples.coffee_bar.bartender.models.events', 'examples.coffee_bar.bartender.models.results',
              'examples.coffee_bar.bartender.models.commands', 'examples.coffee_bar.bartender.models.handlers',
              'examples.coffee_bar.coffeemaker', 'examples.coffee_bar.coffeemaker.models',
              'examples.coffee_bar.coffeemaker.models.events', 'examples.coffee_bar.coffeemaker.models.results',
              'examples.coffee_bar.coffeemaker.models.commands', 'examples.coffee_bar.coffeemaker.models.handlers',
              'examples.coffee_bar.cash_register', 'examples.coffee_bar.cash_register.models',
              'examples.coffee_bar.cash_register.models.events', 'examples.coffee_bar.cash_register.models.handlers',
              'examples.coffee_bar.cash_register.interfaces', 'examples.coffee_bar.cash_register.interfaces.rest',
              'migration', 'migration.app'],
    url='',
    license='',
    author='qotto',
    author_email='contact@qotto.net',
    description=''
)
