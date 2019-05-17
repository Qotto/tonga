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

from typing import Dict

logger = logging.getLogger(__name__)


class StatefulsetPartitionAssignor(AbstractPartitionAssignor):
    name = 'custom'
    version = 0
    assignors_data: bytes = b''

    @classmethod
    def assign(cls, cluster: ClusterMetadata, members: Dict[str, ConsumerProtocolMemberMetadata]) \
            -> Dict[str, ConsumerProtocolMemberAssignment]:
        logger.info('Statefulset Partition Assignor')
        logger.debug(f'Cluster = {cluster}\nMembers = {members}')

        # Get all topic
        all_topics = set()
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
        assignment = collections.defaultdict(lambda: collections.defaultdict(list))
        for member_id in members:
            for tp in all_topic_partitions:
                user_data = json.loads(members[member_id].user_data)
                if user_data['assignor_policy'] == 'all':
                    assignment[member_id][tp.topic].append(tp.partition)
                elif user_data['assignor_policy'] == 'only_own':
                    if tp.partition == user_data['instance']:
                        assignment[member_id][tp.topic].append(tp.partition)
                else:
                    raise ValueError
        logger.debug(f'Assignment = {assignment}')

        protocol_assignment = {}
        for member_id in members:
            protocol_assignment[member_id] = ConsumerProtocolMemberAssignment(cls.version,
                                                                              sorted(assignment[member_id].items()),
                                                                              members[member_id].user_data)

        logger.debug(f'Protocol Assignment = {protocol_assignment}')
        return protocol_assignment

    @classmethod
    def metadata(cls, topics):
        return ConsumerProtocolMemberMetadata(cls.version, list(topics), cls.assignors_data)

    @classmethod
    def on_assignment(cls, assignment):
        pass
