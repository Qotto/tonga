![image](../confluent-logo-300-2.png)

# Overview

This `docker-compose.yml` includes all of the Confluent Platform components and shows how you can configure them to interoperate.
For an example of how to use this Docker setup, refer to the Confluent Platform quickstart: https://docs.confluent.io/current/quickstart/index.html

```bash
# Running docker-compose
cd examples/dev_env
docker-compose up -d --build

# Wait a little for brokers to start
sleep 5

# Get the first broker's ID
export BROKER_ID=$(docker ps -f "name=broker1" --format "{{.ID}}")

# Creating topics for testing purposes
export LOCAL_IP="192.168.2.151"
docker exec -it $BROKER_ID bash -c "kafka-topics --zookeeper $LOCAL_IP:2181 --create --topic test-store --partitions 4 --replication-factor 3"
docker exec -it $BROKER_ID bash -c "kafka-topics --zookeeper $LOCAL_IP:2181 --create --topic test-assignor --partitions 4 --replication-factor 3"

# Running tests
tox

# Cleaning up docker volumes
docker-compose down
```
