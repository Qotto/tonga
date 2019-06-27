Changelog
=========

0.1.0 (Not released yet)
^^^^^^^^^^^^^^^^^^^^^^^^
* Added
    + Services
        + Producer
            - All producer now work with the new BasePositioning class
        + Consumer
            - All consumer now work with the new BasePositioning class
        + Coordinator
            + Client
                - New concept BaseClient, user for initialize Consumer / Producer / StoreManager
                - New class KafkaClient, used for initialize KafkaConsumer / KafkaProducer / KafkaStoreManager, (Primitive Obsession refactor)
            + Transaction
                - New concept BaseTransactionManager & BaseTransactionContext
                - New class KafkaTransactionManager & KafkaTransactionContext
    + Stores
        - New concept BaseStoreManager (Manage local & global store)
        + Metadata
            - New concept BaseStoreMetaData (Used by stores for message bus positioning)
            - New class KafkaStoreMetaData (Used for save positioning values for kafka)
    + Models
        + Record
            - In BaseRecord two serialization abstract method (to_dict / from_dict) | new method base_dict (return base class in dict)
            - In BaseEvent two serialization abstract method (to_dict / from_dict)
            - In BaseCommand two serialization abstract method (to_dict / from_dict) | new method base_dict (return base class in dict)
            - In BaseResult two serialization abstract method (to_dict / from_dict) | new method base_dict (return base class in dict)
        + Store
            - In StoreRecord two serialization method (to_dict / from_dict)
        + Structure
            - New concept BasePositioning (manage topic partition offset)
            - New structs KafkaPositioning (replace TopicPartition namedtuple)
            - New concept StoreRecordType used by StoreManager (new StoreRecord operation_type 'set/del') (Primitive Obsession refactor)
    + General
        - More documentations
        - More tests

* Changed
    + Models
        - Renamed events package to records
        - Changed (BaseRecord, BaseEvent, BaseCommand, BaseResult, StoreRecord) serialization logic
        + Store
            - Changed 'ctype' variable in BaseStoreRecord to 'operation_type' (type : StoreRecordType)
            - Moved BaseStoreRecord in models.records
            - Moved StoreRecords in models.records.store
            - Moved BaseStoreRecordHandler in models.handler
            - Moved StoreRecordHandler in models.handlers.store
    + Services
        + Producer
            - BaseProducer inherit form ABCMeta
            - All producer work with the new BasePositioning class
            - Changed KafkaProducer constructor params (removed name, bootstrap_servers)
            - Renamed KafkaProducer 'send_and_await' method  to 'send_and_wait'
            - Renamed KafkaProducer 'send_and_await' method return (BasePositioning)
            - Changed KafkaProducer 'send' method return (Awaitable)
        + Consumer
            - BaseConsumer inherit form ABCMeta
            - All consumer work with the new BasePositioning class
            - Changed KafkaConsumer constructor params (removed name, bootstrap_servers)
            - Renamed KafkaConsumer 'listen_event' method to 'listen_records'
        + Serializer
            - AvroSerializer handler_class is now optional in register_class function
    + Stores
        + Manager
            - Renamed BaseStoreBuilder to BaseStoreManager
            - BaseStoreManager inherit form ABCMeta
            - Renamed StoreBuilder to KafkaStoreManager
            - StoreManager constructor params (removed current_instance, nb_replica, bootstrap_server, cluster_metadata, cluster_admin)
            - 'set_from_local_store' method from KafkaStoreManager await local store initialization
            - 'get_from_local_store' method from KafkaStoreManager await local store initialization
            - 'delete_from_local_store' method from KafkaStoreManager await local store initialization
        + Local & global
            - BaseStore inherit form ABCMeta
            - Renamed global store package (globall -> global_store)
            - Renamed local store package (local -> local_store)
            - LocalStoreMemory is now compatible with all consumer (Abstract positioning)
            - GlobalStoreMemory is now compatible with all consumer (Abstract positioning)
    + General
        - Update aiokafka version 0.5.1 -> 0.5.2

* Removed
    + Models
        - Removed store_record folder (moved in records / handlers)
        - Removed StoreRecordBase
    + General
        - Removed all privates variables from documentation
        - Removed all privates methods from documentation

* Fixed
    - Some StoreManager bug
    - Store initialization failure
    - Waiter project example
    - Cash-register project example
    - KafkaConsumer crash (without group_id and try to seek committed)
    - KafkaConsumer & KafkaStoreManager bug (Fail to init store if topic / partition have only one record)
    - Tests (new concept adaptation)

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
