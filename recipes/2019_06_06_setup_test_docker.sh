#!/bin/bash

# Wait a little for brokers to start
	sleep 5
	# Get the first broker's ID
	BROKER_ID=$(docker ps -f "name=broker1" --format "{{.ID}}")
	# Ask for local IP address
	SED_PATTERN='s/.*inet \(.*\) netmask.*/\1/'
	IPS=$(ifconfig | grep 'inet ' | sed "$SED_PATTERN")
	PS3="Choose your local IP address: "
	select LOCAL_IP in $IPS exit
	do
        case $LOCAL_IP in
            exit)
                break
                ;;
            *)
                docker exec -it $BROKER_ID bash -c "kafka-topics --zookeeper $LOCAL_IP:2181 --create --topic test-store --partitions 4 --replication-factor 3"
                docker exec -it $BROKER_ID bash -c "kafka-topics --zookeeper $LOCAL_IP:2181 --create --topic test-assignor --partitions 4 --replication-factor 3"
                ;;
        esac
	done