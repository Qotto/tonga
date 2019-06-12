#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" StatefulsetPartitionAssignor

This class assign consumer on right partition. But if you have more partition then instance some partition was not
assigned on consumer, if you have more consumer then partition an errors was raised (this feature will be write later
don't worry)

Examples:
    I want 4 instance of waiter coffee, StatefulsetPartitionAssignor assign each waiter instance on Kafka partition
    by instance numbers

    waiter 1 -------> Topic coffee-ordered | partition 1
    waiter 2 -------> Topic coffee-ordered | partition 2
    waiter 3 -------> Topic coffee-ordered | partition 3
    waiter 4 -------> Topic coffee-ordered | partition 4

Todo:
    * Add repartition
"""

import collections
import json
import logging
from typing import Dict, DefaultDict, Any, Set, List

from kafka import TopicPartition
from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment

from tonga.services.coordinator.assignors.errors import BadAssignorPolicy


class StatefulsetPartitionAssignor(AbstractPartitionAssignor):
    """ Assigns consumer, two mod are available in assignor data (only_own, all).

        - *only_own* : assign consumer on same partition as its instance number
        - *all* : Assign consumer on all topic partitions

    Attributes:
        name (str): Assignor name
        version (int): Assignor version
        assignors_data (bytes): Bytes dict which contains all information for assign consumer
        logger (Logger): StatefulsetPartitionAssignor logger
    """
    name = 'StatefulsetPartitionAssignor'
    version = 0
    assignors_data: bytes = b''
    logger = logging.getLogger('tonga')

    def __init__(self, assignors_data: bytes) -> None:
        """StatefulsetPartitionAssignor constructor

        Args:
            assignors_data (bytes): Bytes dict which contains all information for assign consumer

        Returns:
            None
        """
        self.assignors_data = assignors_data

    def assign(self, cluster: ClusterMetadata, members: Dict[str, ConsumerProtocolMemberMetadata]) \
            -> Dict[str, ConsumerProtocolMemberAssignment]:
        """Assign function was call by aiokafka for assign consumer on right topic partition.

        Args:
            cluster (ClusterMetadata):  Kafka-python cluster metadata (more detail in kafka-python documentation)
            members (Dict[str, ConsumerProtocolMemberMetadata]): members dict which contains
                                                                ConsumerProtocolMemberMetadata
                                                                (more detail in kafka-python documentation)

        Returns:
            Dict[str, ConsumerProtocolMemberAssignment]: dict which contain members and assignment protocol (more detail
                                                         in kafka-python documentation)
        """
        self.logger.info('Statefulset Partition Assignor')
        self.logger.debug('Cluster = %s\nMembers = %s', cluster, members)

        # Get all topic
        all_topics: Set = set()
        for key, metadata in members.items():
            self.logger.debug('Key = %s\nMetadata = %s', key, metadata)
            all_topics.update(metadata.subscription)

        # Get all partitions by topic name
        all_topic_partitions = []
        for topic in all_topics:
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                self.logger.warning('No partition metadata for topic %s', topic)
                continue
            for partition in partitions:
                all_topic_partitions.append(TopicPartition(topic, partition))
        # Sort partition
        all_topic_partitions.sort()

        # Create default dict with lambda
        assignment: DefaultDict[str, Any] = collections.defaultdict(lambda: collections.defaultdict(list))

        advanced_assignor_dict = self.get_advanced_assignor_dict(all_topic_partitions)

        for topic, partitions in advanced_assignor_dict.items():
            for member_id, member_data in members.items():
                # Loads member assignors data
                user_data = json.loads(member_data.user_data)
                # Get number of partitions by topic name
                topic_number_partitions = len(partitions)

                # Logic assignors if nb_replica as same as topic_numbers_partitions (used by StoreBuilder for
                # assign each partitions to right instance, in this case nb_replica is same as topic_number_partitions)
                if user_data['nb_replica'] == topic_number_partitions:
                    if user_data['assignor_policy'] == 'all':
                        for partition in partitions:
                            assignment[member_id][topic].append(partition)
                    elif user_data['assignor_policy'] == 'only_own':
                        if user_data['instance'] in partitions:
                            assignment[member_id][topic].append(partitions[user_data['instance']])
                    else:
                        raise BadAssignorPolicy

                else:
                    raise NotImplementedError

        self.logger.debug('Assignment = %s', assignment)

        protocol_assignment = {}
        for member_id in members:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment(self.version,
                                                                              sorted(assignment[member_id].items()),
                                                                              members[member_id].user_data)

        self.logger.debug('Protocol Assignment = %s', protocol_assignment)
        return protocol_assignment

    @staticmethod
    def get_advanced_assignor_dict(all_topic_partitions: List[TopicPartition]) -> Dict[str, List[int]]:
        """ Transform List[TopicPartition] to Dict[str, List[int]]


        Args:
            all_topic_partitions (List[TopicPartition]): List of TopicPartition

        Returns:
            Dict[str, List[int]]: dict contain topics and partitions in list
        """
        result: Dict[str, List[int]] = dict()
        for tp in all_topic_partitions:
            if tp.topic not in result:
                result[tp.topic] = list()
            result[tp.topic].append(tp.partition)
        return result

    def metadata(self, topics):
        """


        Args:
            topics:

        Returns:

        """
        return ConsumerProtocolMemberMetadata(self.version, list(topics), self.assignors_data)

    @classmethod
    def on_assignment(cls, assignment: ConsumerProtocolMemberAssignment) -> None:
        """This method was call by kafka-python after topic/partitions assignment

        Args:
            assignment (ConsumerProtocolMemberAssignment): more detail in kafka-python documentation

        Returns:
            None
        """
