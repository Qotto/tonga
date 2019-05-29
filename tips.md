# Kafka Tips

#### Read all Kafka msg by topic name
````bash
kafka-console-consumer --bootstrap-server <broker_host>:9092 --topic test --from-beginning
````

#### Speed down / clean / up
````bash
docker-compose stop ; docker-compose rm ; docker volume rm dev_env_kafka_data ; docker volume rm dev_env_zoo_log_data ; docker volume rm dev_env_zoo_data; docker-compose up -d --build
````

#### Describe consumer-group
````bash
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group test-consumer-group
````

#### Create topic
````bash
kafka-topics.sh --zookeeper localhost:2181 --create --topic remove-me --partitions 1 --replication-factor 1
````

#### List all topics
````bash
kafka-topics.sh --zookeeper localhost:2181 --list
````

#### Describe topic
```bash
kafka-topics.sh --zookeeper localhost:2181 --describe --topic remove-me
```

#### Remove topic 
Add ``delete.topic.enable=true`` in server.properties file
````bash
kafka-topics.sh --zookeeper localhost:2181 --delete --topic remove-me
````


#### Topics for coffee bar example
kafka-topics --zookeeper 192.168.2.145:2181 --create --topic waiter-events --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic waiter-stores --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic cash-register-events --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic cash-register-stores --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic bartender-events --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic coffee-maker-events --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic coffee-maker-commands --partitions 2 --replication-factor 3

kafka-topics --zookeeper 192.168.2.145:2181 --create --topic coffee-maker-results --partitions 2 --replication-factor 3
