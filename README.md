# **AioEvent**

Asyncio client for event driven app

## Summarize:

aioevent is a client for build event driven app with Apache Kafka distributed stream processing system using asyncio. It is based on the aiokafka.
aiovent was thought to be deployed in a Kubernetes cluster. aioevent can be used with 0.9+ Kafka brokers

## Requirements:

For development
```text
python  version : v3.7.3
pipenv  version : 2018.11.26
```

For aioevent testing
```text
docker              version : 19.03.0-beta2, build c601560
docker-compose      version : v1.23.2, build 1110ad01
```

## Dependencies:
 
For development
````text
aiokafka==0.5.1
avro-python3==1.8.2
pip==19.1
PyYAML==5.1
sanic==19.3.1
setuptools==41.0.1
wheel==0.33.1
````

For aioevent testing
````text
aiohttp==3.5.4
mypy==0.701
tox==3.9.0
````

## Overview: 

Aioevent defines components to help building an event-driven app:

* Model events / commands / results
* Serializer events / commands / results
* Consume events / commands / results
* Produce events / commands / results
* With or without HTTP handler
* Asyncio

The module provides some implementations:

* Avro schema serializer
* Kafka consumer / producer
* Sanic HTTP request handler


## Pattern:


## Getting started:
