#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import json
import pytest

from tonga.services.coordinator.assignors.statefulset_assignors import StatefulsetPartitionAssignor


def test_metadata_statefulset_assignors(get_assignor_cluster_metadata):
    cluster_metadata = get_assignor_cluster_metadata

    assignors_data = {'instance': 0,
                      'nb_replica': 4,
                      'assignor_policy': 'all'}
    statefulset_assignor = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data), 'utf-8'))

    assignors_metadata = statefulset_assignor.metadata(['test-assignor'])
    assert assignors_metadata.__dict__['version'] == 0
    assert assignors_metadata.__dict__['subscription'] == ['test-assignor']
    assert assignors_metadata.__dict__['user_data'] == b'{"instance": 0, "nb_replica": 4, "assignor_policy": "all"}'


def test_assign_statefulset_assignors_mode_full(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    assignors_data = {'instance': 0,
                      'nb_replica': 4,
                      'assignor_policy': 'all'}
    statefulset_assignor = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data), 'utf-8'))

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    assignors_metadata = statefulset_assignor.metadata(['test-assignor'])

    assignor_dict = {'client1': assignors_metadata}

    member_assignment = statefulset_assignor.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment = member_assignment['client1'].__dict__

    assert consumer_protocol_member_assignment['version'] == 0
    assert consumer_protocol_member_assignment['assignment'] == [('test-assignor', [0, 1, 2, 3])]
    assert consumer_protocol_member_assignment['user_data'] == b'{"instance": 0, "nb_replica": 4, ' \
                                                               b'"assignor_policy": "all"}'


def test_assign_statefulset_assignors_mode_only_own(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    assignors_data = {'instance': 0,
                      'nb_replica': 4,
                      'assignor_policy': 'only_own'}
    statefulset_assignor = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data), 'utf-8'))

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    assignors_metadata = statefulset_assignor.metadata(['test-assignor'])

    assignor_dict = {'client1': assignors_metadata}

    member_assignment = statefulset_assignor.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment = member_assignment['client1'].__dict__

    assert consumer_protocol_member_assignment['version'] == 0
    assert consumer_protocol_member_assignment['assignment'] == [('test-assignor', [0])]
    assert consumer_protocol_member_assignment['user_data'] == b'{"instance": 0, "nb_replica": 4, ' \
                                                               b'"assignor_policy": "only_own"}'


def test_assign_statefulset_assignors_mode_full_4_client(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    # Client 0
    assignors_data0 = {'instance': 0, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor0 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data0), 'utf-8'))
    assignors_metadata0 = statefulset_assignor0.metadata(['test-assignor'])

    # Client 1
    assignors_data1 = {'instance': 1, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor1 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data1), 'utf-8'))
    assignors_metadata1 = statefulset_assignor1.metadata(['test-assignor'])

    # Client 2
    assignors_data2 = {'instance': 2, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor2 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data2), 'utf-8'))
    assignors_metadata2 = statefulset_assignor2.metadata(['test-assignor'])

    # Client 3
    assignors_data3 = {'instance': 3, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor3 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data3), 'utf-8'))
    assignors_metadata3 = statefulset_assignor3.metadata(['test-assignor'])

    assignor_dict = {'client1': assignors_metadata0, 'client2': assignors_metadata1,
                     'client3': assignors_metadata2, 'client4': assignors_metadata3}

    member_assignment = statefulset_assignor0.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment0 = member_assignment['client1'].__dict__
    assert consumer_protocol_member_assignment0['version'] == 0
    assert consumer_protocol_member_assignment0['assignment'] == [('test-assignor', [0])]
    assert consumer_protocol_member_assignment0['user_data'] == b'{"instance": 0, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client2'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor', [1])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 1, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment2 = member_assignment['client3'].__dict__
    assert consumer_protocol_member_assignment2['version'] == 0
    assert consumer_protocol_member_assignment2['assignment'] == [('test-assignor', [2])]
    assert consumer_protocol_member_assignment2['user_data'] == b'{"instance": 2, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment3 = member_assignment['client4'].__dict__
    assert consumer_protocol_member_assignment3['version'] == 0
    assert consumer_protocol_member_assignment3['assignment'] == [('test-assignor', [3])]
    assert consumer_protocol_member_assignment3['user_data'] == b'{"instance": 3, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'


def test_assign_statefulset_assignors_mode_full_2_client(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    # Client 0
    assignors_data0 = {'instance': 0, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor0 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data0), 'utf-8'))
    assignors_metadata0 = statefulset_assignor0.metadata(['test-assignor'])

    # Client 1
    assignors_data1 = {'instance': 1, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor1 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data1), 'utf-8'))
    assignors_metadata1 = statefulset_assignor1.metadata(['test-assignor'])

    assignor_dict = {'client1': assignors_metadata0, 'client2': assignors_metadata1}

    member_assignment = statefulset_assignor0.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment0 = member_assignment['client1'].__dict__
    assert consumer_protocol_member_assignment0['version'] == 0
    assert consumer_protocol_member_assignment0['assignment'] == [('test-assignor', [0])]
    assert consumer_protocol_member_assignment0['user_data'] == b'{"instance": 0, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client2'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor', [1])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 1, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'


@pytest.mark.skip(reason='Not yet implemented')
def test_assign_statefulset_assignors_2_instance_4_partitions(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    # Client 0
    assignors_data0 = {'instance': 0, 'nb_replica': 2, 'assignor_policy': 'only_own'}
    statefulset_assignor0 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data0), 'utf-8'))
    assignors_metadata0 = statefulset_assignor0.metadata(['test-assignor-i'])

    # Client 1
    assignors_data1 = {'instance': 1, 'nb_replica': 2, 'assignor_policy': 'only_own'}
    statefulset_assignor1 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data1), 'utf-8'))
    assignors_metadata1 = statefulset_assignor1.metadata(['test-assignor-i'])

    assignor_dict = {'client1': assignors_metadata0, 'client2': assignors_metadata1}

    member_assignment = statefulset_assignor0.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment0 = member_assignment['client1'].__dict__
    assert consumer_protocol_member_assignment0['version'] == 0
    assert consumer_protocol_member_assignment0['assignment'] == [('test-assignor-i', [0, 1])]
    assert consumer_protocol_member_assignment0['user_data'] == b'{"instance": 0, "nb_replica": 2, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client2'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor-i', [2, 3])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 1, "nb_replica": 2, ' \
                                                                b'"assignor_policy": "only_own"}'


@pytest.mark.skip(reason='Not yet implemented')
def test_assign_statefulset_assignors_4_instance_2_partitions(get_assignor_cluster_metadata, get_assignor_kafka_client):
    cluster_metadata = get_assignor_cluster_metadata
    client = get_assignor_kafka_client

    response_metadata = client.poll(future=client.cluster.request_update())
    cluster_metadata.update_metadata(response_metadata[0])

    # Client 0
    assignors_data0 = {'instance': 0, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor0 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data0), 'utf-8'))
    assignors_metadata0 = statefulset_assignor0.metadata(['test-assignor-j'])

    # Client 1
    assignors_data1 = {'instance': 1, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor1 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data1), 'utf-8'))
    assignors_metadata1 = statefulset_assignor1.metadata(['test-assignor-j'])

    # Client 2
    assignors_data2 = {'instance': 2, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor2 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data2), 'utf-8'))
    assignors_metadata2 = statefulset_assignor2.metadata(['test-assignor-j'])

    # Client 3
    assignors_data3 = {'instance': 3, 'nb_replica': 4, 'assignor_policy': 'only_own'}
    statefulset_assignor3 = StatefulsetPartitionAssignor(bytes(json.dumps(assignors_data3), 'utf-8'))
    assignors_metadata3 = statefulset_assignor3.metadata(['test-assignor-j'])

    assignor_dict = {'client1': assignors_metadata0, 'client2': assignors_metadata1,
                     'client3': assignors_metadata2, 'client4': assignors_metadata3}

    member_assignment = statefulset_assignor0.assign(cluster_metadata, assignor_dict)

    consumer_protocol_member_assignment0 = member_assignment['client1'].__dict__
    assert consumer_protocol_member_assignment0['version'] == 0
    assert consumer_protocol_member_assignment0['assignment'] == [('test-assignor-j', [0])]
    assert consumer_protocol_member_assignment0['user_data'] == b'{"instance": 0, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client2'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor-j', [1])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 1, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client3'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor-j', [0])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 2, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'

    consumer_protocol_member_assignment1 = member_assignment['client4'].__dict__
    assert consumer_protocol_member_assignment1['version'] == 0
    assert consumer_protocol_member_assignment1['assignment'] == [('test-assignor-j', [1])]
    assert consumer_protocol_member_assignment1['user_data'] == b'{"instance": 3, "nb_replica": 4, ' \
                                                                b'"assignor_policy": "only_own"}'
