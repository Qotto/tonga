#!/usr/bin/env python
# coding: utf-8
# Copyright (c) Qotto, 2019

import re
import os
import logging
import yaml
import json
from yaml import FullLoader  # type: ignore
from logging import Logger
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader, AvroTypeException
from avro.schema import NamedSchema, Parse
from io import BytesIO

from typing import Dict, Any, Union

from .base import BaseSerializer

from aioevent.models.events.base import BaseModel
from aioevent.models.handler.base import BaseHandler
from aioevent.models.store_record.base import BaseStoreRecordHandler, BaseStoreRecord
from aioevent.models.exceptions import AvroEncodeError, AvroDecodeError


__all__ = [
    'AvroSerializer',
]


class AvroSerializer(BaseSerializer):
    AVRO_SCHEMA_FILE_EXTENSION = 'avsc.yaml'
    logger: Logger
    schemas_folder: str
    _schemas: Dict[str, NamedSchema]
    _events: Dict[object, Dict[str, Union[BaseModel, BaseStoreRecord, BaseHandler, BaseStoreRecordHandler]]]

    def __init__(self, schemas_folder: str):
        super().__init__()
        self.schemas_folder = schemas_folder
        # TODO Remove workaround
        self.schemas_folder_lib = os.path.dirname(os.path.abspath(__file__)) + '/../../models/store_record/avro_schema'
        self.logger = logging.getLogger(__name__)
        self._schemas = dict()
        self._events = dict()
        self._scan_schema_folder(self.schemas_folder)
        self._scan_schema_folder(self.schemas_folder_lib)

    def register_event_handler_store_record(self, store_record_event: BaseStoreRecord,
                                              store_record_handler: BaseStoreRecordHandler):
        event_name_regex = re.compile(store_record_event.event_name())
        self._events[event_name_regex] = {'event_class': store_record_event, 'handler_class': store_record_handler}

    def _scan_schema_folder(self, schemas_folder: str) -> None:
        with os.scandir(schemas_folder) as files:
            for file in files:
                if not file.is_file():
                    continue
                if file.name.startswith('.'):
                    continue
                if not file.name.endswith(f'.{self.AVRO_SCHEMA_FILE_EXTENSION}'):
                    continue
                self._load_schema_from_file(file.path)

    def _load_schema_from_file(self, file_path: str) -> None:
        with open(file_path, 'r') as fd:
            for s in yaml.load_all(fd, Loader=FullLoader):
                avro_schema_data = json.dumps(s)
                avro_schema = Parse(avro_schema_data)
                schema_name = avro_schema.namespace + '.' + avro_schema.name
                if schema_name in self._schemas:
                    raise Exception(f"Avro schema {schema_name} was defined more than once!")
                self._schemas[schema_name] = avro_schema

    def get_schemas(self) -> Dict[str, NamedSchema]:
        return self._schemas

    def get_events(self) -> Dict[object, Dict[str, Union[BaseModel, BaseStoreRecord,
                                                         BaseHandler, BaseStoreRecordHandler]]]:
        return self._events

    def register_class(self, event_name: str, event_class: BaseModel, handler_class: BaseHandler) -> None:
        event_name_regex = re.compile(event_name)

        matched: bool = False
        for schema_name in self._schemas:
            if event_name_regex.match(schema_name):
                matched = True
                break
        if not matched:
            raise NameError(f"{event_name} does not match any schema")
        self._events[event_name_regex] = {'event_class': event_class, 'handler_class': handler_class}

    def encode(self, event: BaseModel) -> bytes:
        schema = self._schemas[event.event_name()]

        if schema is None:
            raise NameError(f"No schema found to encode event with name {event.event_name()}")
        try:
            output = BytesIO()
            writer = DataFileWriter(output, DatumWriter(), schema)
            writer.append(event.__dict__)
            writer.flush()
            encoded_event = output.getvalue()
            writer.close()
        except AvroTypeException as err:
            raise AvroEncodeError(err, 500)
        return encoded_event

    def decode(self, encoded_event: Any) -> Dict[str, Union[BaseModel, BaseHandler]]:
        try:
            reader = DataFileReader(BytesIO(encoded_event), DatumReader())
            schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
            schema_name = schema['namespace'] + '.' + schema['name']
            event_data = next(reader)
        except AvroTypeException as err:
            raise AvroDecodeError(err, 500)

        # finds a matching event name
        event_class = None
        handler_class = None
        for e_name, dict_class in self._events.items():
            if e_name.match(schema_name):  # type: ignore
                event_class = dict_class['event_class']
                handler_class = dict_class['handler_class']
                break
        r_dict = dict()
        r_dict['event_class'] = event_class.from_data(event_data=event_data)
        r_dict['handler_class'] = handler_class
        return r_dict
