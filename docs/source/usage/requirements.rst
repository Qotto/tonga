.. _requirements:

============
Requirements
============

For development:

.. code-block:: python
   :linenos:

   python_version += 'v3.5.1'
   pipenv_version += '2018.11.26'

For testing:

.. code-block:: python
   :linenos:

   docker              version : 19.03.0-beta2, build c601560
   docker-compose      version : v1.23.2, build 1110ad01

============
Dependencies
============

For development:

.. code-block:: python
   :linenos:

   aiokafka==0.5.1
   avro-python3==1.8.2
   pip==19.1
   PyYAML==5.1
   kafka-python==1.4.6

For tonga testing:

.. code-block:: python
   :linenos:

   aiohttp==3.5.4
   tox==3.9.0
   sphinx==2.0.1
   pytest-asyncio==0.10.0
   sanic==19.3.1
