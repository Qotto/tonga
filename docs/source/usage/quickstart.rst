.. _quickstart:

==========
Quickstart
==========

Basic usage

KafkaProducer
-------------

KafkaProducer is a high-level, asynchronous message producer.

.. code-block:: python

   import uvloop
   import asyncio
   import uuid
   import os

   from recipes.setup_color_logger import setup_logger

   from tonga.services.producer.kafka_producer import KafkaProducer
   from tonga.services.serializer.avro import AvroSerializer
   from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner

   from examples.coffee_bar.waiter.models.events.coffee_ordered import CoffeeOrdered
   from examples.coffee_bar.waiter.models.handlers.coffee_ordered_handler import CoffeeOrderedHandler


   async def send_one(kafka_producer):
       coffee_uuid = uuid.uuid4().hex
       coffee_order = CoffeeOrdered(partition_key=coffee_uuid, uuid=coffee_uuid, coffee_for='toto',
                                 coffee_type='Classic', cup_type='Venti', amount=2.5)
       try:
          await kafka_producer.send_and_await(coffee_order, 'waiter-events')
       finally:
          await kafka_producer.stop_producer()


   cur_instance = 0
   logger = setup_logger()

   loop = uvloop.new_event_loop()
   asyncio.set_event_loop(loop)

   # Creates AvroSerializer
   serializer = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         'examples/coffee_bar/avro_schemas'))
   coffee_ordered_handler = CoffeeOrderedHandler()

   # Register CoffeeFinished (event class) / CoffeeFinishedHandler (instanced class handle all CoffeeFinished event)
   serializer.register_class('tonga.waiter.event.CoffeeOrdered', CoffeeOrdered, coffee_ordered_handler)

   # Creates KafkaProducer
   producer = KafkaProducer(name=f'waiter-{cur_instance}', bootstrap_servers='localhost:9092',
                            client_id=f'waiter-{cur_instance}', serializer=serializer,
                            loop=loop, partitioner=KeyPartitioner(), acks='all')

   loop.run_until_complete(send_one(producer))


KafkaConsumer
-------------

.. code-block:: python

   import uvloop
   import asyncio
   import uuid
   import os
   from signal import signal, SIGINT

   from recipes.setup_color_logger import setup_logger

   from tonga.services.consumer.kafka_consumer import KafkaConsumer
   from tonga.services.serializer.avro import AvroSerializer

   from examples.coffee_bar.waiter.models.events.coffee_ordered import CoffeeOrdered
   from examples.coffee_bar.waiter.models.handlers.coffee_ordered_handler import CoffeeOrderedHandler

   cur_instance = 0
   nb_replica = 2
   logger = setup_logger()

   loop = uvloop.new_event_loop()
   asyncio.set_event_loop(loop)

   # Creates AvroSerializer
   serializer = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         'examples/coffee_bar/avro_schemas'))
   coffee_ordered_handler = CoffeeOrderedHandler()

   # Register CoffeeFinished (event class) / CoffeeFinishedHandler (instanced class handle all CoffeeFinished event)
   serializer.register_class('tonga.waiter.event.CoffeeOrdered', CoffeeOrdered, coffee_ordered_handler)

   consumer = KafkaConsumer(name=f'waiter-{cur_instance}', serializer=serializer,
                           bootstrap_servers='localhost:9092', client_id=f'waiter-{cur_instance}',
                           topics=['waiter-events'], loop=loop,
                           assignors_data={'instance': cur_instance,
                                           'nb_replica': nb_replica,
                                           'assignor_policy': 'only_own'}, isolation_level='read_committed')

   # Ensures future of KafkaConsumer
   asyncio.ensure_future(consumer.listen_event('earliest'), loop=loop)

   # Catch SIGINT
   signal(SIGINT, lambda s, f: loop.stop())
   try:
       # Runs forever
       loop.run_forever()
   except Exception:
       # If an exception was raised loop was stopped
       loop.stop()
