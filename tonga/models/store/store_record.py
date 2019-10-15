#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

""" StoreRecord class

More detail in BaseStoreRecord

"""
from datetime import datetime, timezone

from typing import Dict, Any

from tonga.models.structs.store_record_type import StoreRecordType


__all__ = [
    'StoreRecord'
]


class StoreRecord:
    """ Base of all StoreRecord

    Attributes:
        schema_version (str): Includes the schema version of the record, it helps to keep applications compatible
                              with older records in the system
        key (str): Should be a key determining to which partition your record will be assigned.
                   Records with the same *key* value are guaranteed to be written to the same partition and by used for
                   Kafka compaction. Use an UUID for store value
        operation_type (StoreRecordType): Record type (possible value *set* / *del*)
        value (bytes): Record value as bytes format
    """
    schema_version: str
    date: datetime
    key: str
    operation_type: StoreRecordType
    value: bytes

    def __init__(self, key: str, operation_type: StoreRecordType, value: bytes,
                 schema_version: str = None, date: datetime = None) -> None:
        """ BaseStoreRecord constructor

        Args:
            key (str): Should be a key determining to which partition your record will be assigned.
                       Records with the same *key* value are guaranteed to be written to the same partition and by used
                       for Kafka compaction. Use an UUID for store value
            operation_type (StoreRecordType): Record type (possible value set/del)
            value (bytes): Record value as bytes format
            schema_version (str): Includes the schema version of the record, it helps to keep applications compatible
                                  with older records in the system
            date (datetime): Datetime object

        Returns:
            None
        """
        if schema_version is None:
            self.schema_version = '0.0.0'
        else:
            self.schema_version = schema_version

        if date is None:
            self.date = datetime.now(timezone.utc)

        self.key = key
        self.operation_type = operation_type
        self.value = value

    def to_dict(self) -> Dict[str, Any]:
        """ Serialize BaseRecord to dict

        Returns:
            Dict[str, Any]: class in dict format
        """
        return {
            'schema_version': self.schema_version,
            'datetime': self.date.isoformat(),
            'timestamp': self.date.timestamp() * 1000,
            'operation_type': self.operation_type.value,
            'key': self.key,
            'value': self.value
        }

    @classmethod
    def from_dict(cls, dict_data: Dict[str, Any]):
        """ Deserialize dict to BaseRecord

        Args:
            dict_data (Dict|str, Any]): Contains all BaseRecord Class attribute for return an instanced class

        Returns:
            None
        """
        return cls(key=dict_data['key'],
                   operation_type=StoreRecordType(dict_data['operation_type']),
                   value=dict_data['value'],
                   schema_version=dict_data['schema_version'],
                   date=datetime.fromtimestamp(dict_data['timestamp'] / 1000, timezone.utc))

    @classmethod
    def event_name(cls) -> str:
        """ Return store record class name, used by serializer

        Returns:
            str: store record name
        """
        return 'tonga.store.record'
