CHANGELOG
=========

0.0.3 (Not released yet)
^^^^^^^^^^^^^^^^^^^^^^^^
* Added documentations
* Changed events to records
* Moved BaseStoreRecord in models.records
* Moved StoreRecords in models.records.store
* Moved BaseStoreRecordHandler in models.handler
* Moved StoreRecordHandler in models.handlers.store
* Removed store_record folder (moved in records / handlers)
* Fixed some StoreBuilder bug
* Fixed store initialization failure
* Fixed waiter project example
* Fixed cash-register project example

0.0.2 (2019-06-05)
^^^^^^^^^^^^^^^^^^
* Added handler (events / commands / results)
* Added local & global store (only memory)
* Added StoreBuilder
* Added statefulset assignors
* Added key partitioner
* Added statefulset partitioner
* Added tests
* Changed docker-compose in example/dev_env (1 broker -> 3 broker)
* Fixed some consumer bug
* Fixed some producer bug
* Refactor exceptions (more explicit)


0.0.1 (2019-04-30)
^^^^^^^^^^^^^^^^^^
* Initial release
* Added Kafka producer / consumers
* Added Avro serializer
* Added schemas model (events / commands / results)
* Added some example (coffee_bar)
* Added docker-compose in example/dev_env (for testing)
