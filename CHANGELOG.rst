Changelog
=========

0.0.3 (Not released yet)
^^^^^^^^^^^^^^^^^^^^^^^^
* Added
    - More documentations
    - New class KafkaClient, used for initialize KafkaConsumer / KafkaProducer / StoreBuilder, (Primitive Obsession refactor)
    - New type structs StoreRecordType used by StoreBuilder (new StoreRecord type 'set/del') (Primitive Obsession refactor)
    - New structs KafkaPositioning (replace TopicPartition namedtuple)
* Changed
    - Events to records
    - Moved BaseStoreRecord in models.records
    - Moved StoreRecords in models.records.store
    - Moved BaseStoreRecordHandler in models.handler
    - Moved StoreRecordHandler in models.handlers.store
    - KafkaConsumer constructor params (removed name, bootstrap_servers)
    - KafkaProducer constructor params (removed name, bootstrap_servers)
    - KafkaProducer func 'send_and_await' to 'send_and_wait'
    - StoreBuilder constructor params (removed current_instance, nb_replica, bootstrap_server, cluster_metadata, cluster_admin)
    - AvroSerializer handler_class is now optional in register_class function
* Removed
    - Removed store_record folder (moved in records / handlers)
* Fixed
    - Some StoreBuilder bug
    - Store initialization failure
    - Waiter project example
    - Cash-register project example
    - Consumer crash (without group_id and try to seek committed)

0.0.2 (2019-06-05)
^^^^^^^^^^^^^^^^^^
* Added
    - Handler (events / commands / results)
    - Local & global store (only memory)
    - StoreBuilder
    - Statefulset assignors
    - Key partitioner
    - Statefulset partitioner
    - Tests
* Changed:
    - Docker-compose in example/dev_env (1 broker -> 3 broker)
* Fixed
    - Some consumer bug
    - Some producer bug
* Refactored
    - Exceptions (more explicit)


0.0.1 (2019-04-30)
^^^^^^^^^^^^^^^^^^
* Initial release
* Added
    - Kafka producer / consumers
    - Avro serializer
    - Schemas model (events / commands / results)
    - Some example (coffee_bar)
    - Docker-compose in example/dev_env (for testing)
