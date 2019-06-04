#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import logging
import json
import collections

from kafka import TopicPartition
from kafka.cluster import ClusterMetadata
from kafka.coordinator.assignors.abstract import AbstractPartitionAssignor
from kafka.coordinator.protocol import ConsumerProtocolMemberMetadata, ConsumerProtocolMemberAssignment

from typing import Dict, DefaultDict, Any, Set, List

from tonga.services.coordinator.assignors.errors import BadAssignorPolicy

logger = logging.getLogger(__name__)


class StatefulsetPartitionAssignor(AbstractPartitionAssignor):
    name = 'StatefulsetPartitionAssignor'
    version = 0
    assignors_data: bytes = b''

    def __init__(self, assignors_data):
        self.assignors_data = assignors_data

    def assign(self, cluster: ClusterMetadata, members: Dict[str, ConsumerProtocolMemberMetadata]) \
            -> Dict[str, ConsumerProtocolMemberAssignment]:
        logger.info('Statefulset Partition Assignor')
        logger.debug(f'Cluster = {cluster}\nMembers = {members}')

        # Get all topic
        all_topics: Set = set()
        for key, metadata in members.items():
            all_topics.update(metadata.subscription)

        # Get all partitions by topic name
        all_topic_partitions = []
        for topic in all_topics:
            partitions = cluster.partitions_for_topic(topic)
            if partitions is None:
                logger.warning('No partition metadata for topic %s', topic)
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
                    # Todo Add repartition
                    raise NotImplementedError

        logger.debug(f'Assignment = {assignment}')

        protocol_assignment = {}
        for member_id in members:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment(self.version,
                                                                              sorted(assignment[member_id].items()),
                                                                              members[member_id].user_data)

        logger.debug(f'Protocol Assignment = {protocol_assignment}')
        return protocol_assignment

    @staticmethod
    def get_advanced_assignor_dict(all_topic_partitions: List[TopicPartition]) -> Dict[str, List[int]]:
        result: Dict[str, List[int]] = dict()
        for tp in all_topic_partitions:
            if tp.topic not in result:
                result[tp.topic] = list()
            result[tp.topic].append(tp.partition)
        return result

    def metadata(self, topics):
        return ConsumerProtocolMemberMetadata(self.version, list(topics), self.assignors_data)

    @classmethod
    def on_assignment(cls, assignment):
        pass
