.. _examples:

========
Examples
========

In all examples we use coffee bar project (See Tonga examples/coffee_bar folder)

Serializer
----------

Serialize all avro schema to bytes, for register a new schema serializer needs an BaseRecord & BaseHandler.
BaseRecord is a serialize class, BaseHandler is call by consumer for handle records.


Register BaseRecord (event / result / command)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Import serializer
   from tonga.services.serializer.avro import AvroSerializer

   # Import store builder
   from tonga.stores.store_builder.store_builder import StoreBuilder

   # Import KafkaProducer
   from tonga.services.producer.kafka_producer import KafkaProducer

   # Import waiter events
   from examples.coffee_bar.waiter.models.events.coffee_finished import CoffeeFinished

   # Import waiter handlers
   from examples.coffee_bar.waiter.models.handlers.coffee_finished_handler import CoffeeFinishedHandler

   # Creates AvroSerializer
   serializer = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               'examples/coffee_bar/avro_schemas'))

   # Creates StoreBuilder  (More information below StoreBuilder section)
   store_builder = StoreBuilder('Go to StoreBuilder documentation')

   # Creates transactional producer (More information below Transactional producer section)
   transactional_producer = KafkaProducer('Go to KafkaProducer documentation')

   # Creates CoffeeFinishedHandler (Waiter example)
   coffee_finished_handler = CoffeeFinishedHandler(store_builder, transactional_producer)

   # Register CoffeeFinished (event class) / CoffeeFinishedHandler (instanced class handle all CoffeeFinished event)
   serializer.register_class('tonga.waiter.event.CoffeeFinished', CoffeeFinished, coffee_finished_handler)


Register StoreRecord
^^^^^^^^^^^^^^^^^^^^

This is an example with Tonga StoreRecordHandler

.. code-block:: python

   # Import serializer
   from tonga.services.serializer.avro import AvroSerializer

   # Import store builder
   from tonga.stores.store_builder.store_builder import StoreBuilder

   # Import StoreRecord & StoreRecordHandler
   from tonga.models.store_record.store_record import StoreRecord
   from tonga.models.store_record.store_record_handler import StoreRecordHandler

   # Creates AvroSerializer
   serializer = AvroSerializer(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              'examples/coffee_bar/avro_schemas'))

   # Creates StoreBuilder  (More information below StoreBuilder section)
   store_builder = StoreBuilder('Go to StoreBuilder documentation')

   # Creates StoreRecordHandler (This handler consumer store record)
   store_record_handler = StoreRecordHandler(store_builder)

   # Register StoreRecord (Record class) and StoreRecordHandler (instanced class which handle StoreRecord)
   serializer.register_event_handler_store_record(StoreRecord, store_record_handler)

Consumer / Producer
-------------------

- Producer send BaseRecord (event / command / result / store) in a Kafka topic.
- Consumer receive BaseRecord (event / command / result / store) from a Kafka topic.

.. code-block:: python

   import uvloop
   from signal import signal, SIGINT

   # Import KafkaProducer / KafkaConsumer
   from tonga.services.consumer.kafka_consumer import KafkaConsumer
   from tonga.services.producer.kafka_producer import KafkaProducer

   # Import key partitioner
   from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner

   cur_instance = 0

   loop = uvloop.new_event_loop()
   asyncio.set_event_loop(loop)

   # Creates KafkaProducer
   producer = KafkaProducer(name=f'waiter-{cur_instance}', bootstrap_servers='localhost:9092',
                                            client_id=f'waiter-{cur_instance}', serializer=serializer,
                                            loop=waiter_app['loop'], partitioner=KeyPartitioner(),
                                            acks='all')

   # Creates KafkaConsumer
   consumer = KafkaConsumer(name=f'waiter-{cur_instance}', serializer=serializer,
                             bootstrap_servers='localhost:9092', client_id=f'waiter-{cur_instance}',
                             topics=['bartender-events'], loop=loop, group_id='waiter',
                             assignors_data={'instance': cur_instance,
                                             'nb_replica': nb_replica,
                                             'assignor_policy': 'only_own'}, isolation_level='read_committed')

   # Ensures future of KafkaConsumer
   asyncio.ensure_future(consumer.listen_event('committed'), loop=loop)

   # Catch SIGINT
   signal(SIGINT, lambda s, f: loop.stop())
   try:
       # Runs forever
       loop.run_forever()
   except Exception:
       # If an exception was raised loop was stopped
       loop.stop()

Transactional Producer
----------------------

.. warning::
   Transactional producer can't send message on Kafka if is not in a transaction.

.. code-block:: python

   import uvloop
   from signal import signal, SIGINT

   # Import KafkaProducer / KafkaConsumer
   from tonga.services.consumer.kafka_consumer import KafkaConsumer
   from tonga.services.producer.kafka_producer import KafkaProducer

   # Import key partitioner
   from tonga.services.coordinator.partitioner.key_partitioner import KeyPartitioner

   cur_instance = 0

   # Creates event loop
   loop = uvloop.new_event_loop()
   asyncio.set_event_loop(loop)

   # Creates transactional KafkaProducer
   producer = KafkaProducer(name=f'waiter-{cur_instance}', bootstrap_servers='localhost:9092',
                                            client_id=f'waiter-{cur_instance}', serializer=serializer,
                                            loop=waiter_app['loop'], partitioner=KeyPartitioner(),
                                            acks='all', transactional_id=f'waiter')

Make transaction
^^^^^^^^^^^^^^^^

Transaction example from waiter project (tonga/example/coffee-bar/waiter)

.. code-block:: python

   from aiokafka import TopicPartition

   # Import BaseCommandHandler
   from tonga.models.handlers.command.command_handler import BaseCommandHandler
   # Import BaseCommand
   from tonga.models.records.command.command import BaseCommand
   # Import BaseProducer
   from tonga.services.producer.base import BaseProducer

   from typing import Union
   # Import MakeCoffeeResult / CoffeeStarted event
   from examples.coffee_bar.coffeemaker.models.results.make_coffee_result import MakeCoffeeResult
   from examples.coffee_bar.coffeemaker.models.events.coffee_started import CoffeeStarted


   class MakeCoffeeHandler(BaseCommandHandler):
       _transactional_producer: BaseProducer

       def __init__(self, transactional_producer: BaseProducer, **kwargs) -> None:
           super().__init__(**kwargs)
           self._transactional_producer = transactional_producer

       async def execute(self, command: BaseCommand, tp: TopicPartition, group_id: str, offset: int) -> Union[str, None]:

          if not self._transactional_producer.is_running():
              await self._transactional_producer.start_producer()

           async with self._transactional_producer.init_transaction():
               # Creates commit_offsets dict

              commit_offsets = {tp: offset + 1}
                # Creates CoffeeStarted event and MakeCoffeeResult result
               coffee_started = CoffeeStarted(command.uuid, context=command.context)
               make_coffee_result = MakeCoffeeResult(command.uuid, context=command.context)

                # Sends CoffeeFinished event
               await self._transactional_producer.send_and_await(coffee_started, 'coffee-maker-events')
               await self._transactional_producer.send_and_await(make_coffee_result, 'coffee-maker-results')

                # End transaction
               await self._transactional_producer.end_transaction(commit_offsets, group_id)
           return 'transaction'

       @classmethod
       def handler_name(cls) -> str:
           return 'tonga.coffeemaker.command.MakeCoffee'


StoreBuilder
------------

Store builder need an AvroSerializer which contains a StoreRecord and StoreRecordHandler

.. code-block:: python

    import uvloop
    import asyncio
    from kafka import KafkaAdminClient
    from kafka.cluster import ClusterMetadata

    # Import local & global store memory
    from tonga.stores.local.memory import LocalStoreMemory
    from tonga.stores.globall.memory import GlobalStoreMemory
    # Import store builder
    from tonga.stores.store_builder.store_builder import StoreBuilder

    cur_instance = 0
    nb_replica = 2

    # Creates event loop
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    # Creates local store memory / global store memory
    local_store = LocalStoreMemory(name=f'waiter-{cur_instance}-local-memory')
    global_store = GlobalStoreMemory(name=f'waiter-{cur_instance}-global-memory')

    cluster_admin = KafkaAdminClient(bootstrap_servers='localhost:9092', client_id=f'waiter-{cur_instance}')
    cluster_metadata = ClusterMetadata(bootstrap_servers='localhost:9092')

    # Creates store builder
    store_builder = StoreBuilder(name=f'waiter-{cur_instance}-store-builder', current_instance=cur_instance,
                                nb_replica=nb_replica, topic_store='waiter-stores', serializer=serializer,
                                local_store=local_store, global_store=global_store,
                                bootstrap_server='localhost:9092', cluster_metadata=cluster_metadata,
                                cluster_admin=cluster_admin, loop=loop, rebuild=True, event_sourcing=False)

    # Ensures future of KafkaConsumer store builder
    store_builder.return_consumer_task()

    # Catch SIGINT
    signal(SIGINT, lambda s, f: loop.stop())
    try:
       # Runs forever
       loop.run_forever()
    except Exception:
       # If an exception was raised loop was stopped
       loop.stop()
