# Kafka Tips

This commands read all Kafka msg by topic name
````bash
kafka-console-consumer --bootstrap-server <broker_host>:9092 --topic test --from-beginning
````

Speed down / clean / up
````bash
docker-compose stop ; docker-compose rm ; docker volume rm dev_env_kafka_data ; docker volume rm dev_env_zoo_log_data ; docker volume rm dev_env_zoo_data; docker-compose up -d --build
````

````bash
kafka-consumer-groups.sh --bootstrap-server 127.0.0.1:9092 --describe --group test-consumer-group
````