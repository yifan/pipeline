🔀pipeline🔀
============
.. image:: https://badge.fury.io/py/tanbih-pipeline.svg
    :target: https://badge.fury.io/py/tanbih-pipeline
.. image:: https://readthedocs.org/projects/tanbih-pipeline/badge/?version=latest
    :target: https://tanbih-pipeline.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://sonarcloud.io/api/project_badges/measure?project=yifan_pipeline&metric=sqale_rating
    :target: https://sonarcloud.io/api/project_badges/measure?project=yifan_pipeline&metric=sqale_rating
    :alt: Maintainability Score


a flexible stream processing framework supporting RabbitMQ, Pulsar, Kafka and Redis.

Features
--------

- at-least-once guaranteed with acknowledgement on every message
- horizontally scalable through consumer groups
- flow is controlled in deployment, develop it once, use it everywhere
- testability provided with FILE and MEMORY input/output

Requirements
------------

- Python 3.8

Installation
------------

.. code-block:: bash

    $ pip install tanbih-pipeline

You can install the required backend dependencies with:

.. code-block:: bash

    $ pip install tanbih-pipeline[redis]
    $ pip install tanbih-pipeline[kafka]
    $ pip install tanbih-pipeline[pulsar]
    $ pip install tanbih-pipeline[rabbitmq]
    $ pip install tanbih-pipeline[elastic]
    $ pip install tanbih-pipeline[mongodb]

If you want to support all backends, you can:

.. code-block:: bash

    $ pip install tanbih-pipeline[full]



Producer
---------

Producer is to be used when developing a data source in our pipeline. A source
will produce output without input. A crawler can be seen as a producer.

.. code-block:: python

    >>> from typing import Generator
    >>> from pydantic import BaseModel
    >>> from pipeline import Producer as Worker, ProducerSettings as Settings
    >>>
    >>> class Output(BaseModel):
    ...     key: int
    >>>
    >>> class MyProducer(Worker):
    ...     def generate(self) -> Generator[Output, None, None]:
    ...         for i in range(10):
    ...             yield Output(key=i)
    >>>
    >>> settings = Settings(name='producer', version='0.0.0', description='')
    >>> producer = MyProducer(settings, output_class=Output)
    >>> producer.parse_args("--out-kind MEM --out-topic test".split())
    >>> producer.start()
    >>> [r.get('key') for r in producer.destination.results]
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


Processor
---------

Processor is to be used to process input. Modification will be in-place. A processor
can produce one output for each input, or no output.

.. code-block:: python

    >>> from pipeline import Processor as Worker, ProcessorSettings as Settings
    >>>
    >>> class Input(BaseModel):
    ...     key: int
    >>>
    >>> class Output(BaseModel):
    ...     key: int
    ...     processed: bool
    >>>
    >>> class MyProcessor(Worker):
    ...     def process(self, input):
    ...         return Output(key=input.key, processed=True)
    >>>
    >>> settings = Settings(name='processor', version='0.1.0', description='')
    >>> processor = MyProcessor(settings, input_class=Input, output_class=Output)
    >>> args = "--in-kind MEM --in-topic test --out-kind MEM --out-topic test".split()
    >>> processor.parse_args(args)
    >>> processor.start()


Splitter
--------

Splitter is to be used when writing to multiple outputs. It will take a function to
generate output topic based on the processing message, and use it when writing output.

.. code-block:: python

    >>> from pipeline import Splitter as Worker, SplitterSettings as Settings
    >>>
    >>> class MySplitter(Worker):
    ...     def get_topic(self, msg):
    ...         return '{}-{}'.format(self.destination.topic, msg.get('id'))
    >>>
    >>> settings = Settings(name='splitter', version='0.1.0', description='')
    >>> splitter = MySplitter(settings)
    >>> args = "--in-kind MEM --in-topic test --out-kind MEM --out-topic test".split()
    >>> splitter.parse_args(args)
    >>> splitter.start()


Usage
-----

## API Server

API

## ETL

Data pipeline 

## Database Record



Pipeline allows your data pipeline to support different technologies. 

+-----------+----------------+---------+--------+-------+
|           |                |  multi- | shared | data  |
| kind      |  description   |  reader | reader | expire|
+===========+================+=========+========+=======+
| LREDIS    |  Redis List    |    X    |    X   | read  |
+-----------+----------------+---------+--------+-------+
| XREDIS    |  Redis Stream  |    X    |    X   | limit |
+-----------+----------------+---------+--------+-------+
| KAFKA     |  Kafka         |    X    |    X   | read  |
+-----------+----------------+---------+--------+-------+
| PULSAR    |  Pulsar        |    X    |    X   | ttl   |
+-----------+----------------+---------+--------+-------+
| RABBITMQ  |  RabbitMQ      |    X    |        | read  |
+-----------+----------------+---------+--------+-------+
| ELASTIC   |  ElasticSearch |         |        |       |
+-----------+----------------+---------+--------+-------+
| MONGODB   |  MongoDB       |         |        |       |
+-----------+----------------+---------+--------+-------+

Writing a Worker
################


Choose Producer, Processor or Splitter to subclass from.




Environment Variables
*********************

Application accepts following environment variables 
(Please note, you will need to add prefix `IN_`, `--in-` and
`OUT_`, `--out-` to these variables to indicate the option for
input and output):

+----------------+-----------------+---------------------+
|   environment  |  command line   |                     |
|   variable     |  argument       | options             |
+================+=================+=====================+
|   KIND         |  --kind         | KAFKA, PULSAR, FILE |
+----------------+-----------------+---------------------+
|   PULSAR       |  --pulsar       | pulsar url          |
+----------------+-----------------+---------------------+
|   TENANT       |  --tenant       | pulsar tenant       |
+----------------+-----------------+---------------------+
|   NAMESPACE    |  --namespace    | pulsar namespace    |
+----------------+-----------------+---------------------+
|   SUBSCRIPTION |  --subscription | pulsar subscription |
+----------------+-----------------+---------------------+
|   KAFKA        |  --kafka        | kafka url           |
+----------------+-----------------+---------------------+
|   GROUPID      |  --group-id     | kafka group id      |
+----------------+-----------------+---------------------+
|   TOPIC        |  --topic        | topic to read       |
+----------------+-----------------+---------------------+


Custom Code
***********

Define add_arguments to add new arguments to worker.

Define setup to run initialization code before worker starts processing messages. setup is called after
command line arguments have been parsed. Logic based on options (parsed arguments) goes here.


Options
*******


Errors
******

The value `None` above is error you should return if `dct` or `dcts` is empty.
Error will be sent to topic `errors` with worker information.


Contribute
----------

Use `pre-commit` to run `black` and `flake8`


Credits
-------

Yifan Zhang (yzhang at hbku.edu.qa)
