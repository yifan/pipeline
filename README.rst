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


Pipeline provides an unified interface to set up data stream processing systems with Kafka, Pulsar,
RabbitMQ, Redis and many more. The idea is to free developer from the dynamic change of technology
in deployment, so that a docker image released for a certain task can be used with Kafka or Redis
through changes of environment variables.



Features
--------

- a unified interface from Kakfa to Pulsar, from Redis to MongoDB
- components connection controlled via command line, or environment variables
- support file and in-memory for testing



Requirements
------------

- Python 3.7, 3.8



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

    >>> from pydantic import BaseModel
    >>> from pipeline import Processor as Worker, ProcessorSettings as Settings
    >>>
    >>> class Input(BaseModel):
    ...     temperature: float
    >>>
    >>> class Output(BaseModel):
    ...     is_hot: bool
    >>>
    >>> class MyProcessor(Worker):
    ...     def process(self, content, key):
    ...         is_hot = (content.temperature > 25)
    ...         return Output(is_hot=is_hot)
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

Choosing backend technology:

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
| FILE*     |  json,csv      |         |        |       |
+-----------+----------------+---------+--------+-------+
| MEM*      |  memory        |         |        |       |
+-----------+----------------+---------+--------+-------+

* FILE accepts jsonl input on stdin and with filename, it also accepts csv file. 
  Both format can be gzipped.
* MEM read and write to memory, designed for unit tests.


.. code-block:: shell

    # check command line arguments for certain input and output
    worker.py --in-kind FILE --help
    # or
    IN_KIND=FILE worker.py
    # or
    export IN_KIND=FILE
    worker.py --help

    # process input from file and output to stdout (--in-content-only is
    # needed for this version)
    worker.py --in-kind FILE --in-filename data.jsonl --in-content-only \
              --out-kind FILE --out-filename -


    # read from file and write to KAFKA
    worker.py --in-kind FILE --in-filename data.jsonl --in-content-only \
              --out-kind KAFKA --out-namespace test --out-topic articles \
              --out-kafka kafka_url --out-config kafka_config_json


Arguments
*********

common
    debug
    monitoring

    kind
    namespace
    topic


input:


FILE




Scripts
*******

`pipeline-copy` is a script to copy data from a source to a destination. It can
be used to inject data from a file to a database, or from a database to another
database. It is implemented as a Pipeline worker.

Since JSON format does not support datetimes, in order for `pipeline-copy` to
treat datetime field as datetime instead of string, you can provide a model
definition via argument `--model-definition`. An example of such model definition
is as following (the class name needs to be `Model`):

.. code-block:: shell

    from datetime import datetime
    from typing import Optional

    from pydantic import BaseModel

    class Model(BaseModel):
        hashtag: str
        username: str
        text: str
        tweet_id: str
        location: Optional[str]
        created_at: datetime
        retweet_count: int



Environment Variables
*********************

Application accepts following environment variables
(Please note, you will need to add prefix `IN_`, `--in-` and
`OUT_`, `--out-` to these variables to indicate the option for
input and output). Please refer to backend documentation for
available arguments/environment variables.


Customize Settings
******************

.. code-block:: python

    class CustomSettings(Settings):
        new_argument: str = Field("", title="a new argument for custom settings")

    class CustomProcessor(Processor):
        def __init__(self):
            settings = CustomSettings("worker", "v0.1.0", "custom processor")
            super().__init__(settings, input_class=BaseModel, output_class=BaseModel)



Errors
******

PipelineError will be raised when error occurs 


Contribute
----------

Use `pre-commit` to run `black` and `flake8`


Credits
-------

Yifan Zhang (yzhang at hbku.edu.qa)
