🔀pipeline🔀
============
.. image:: https://badge.fury.io/py/tanbih-pipeline.svg
    :target: https://badge.fury.io/py/tanbih-pipeline
.. image:: https://readthedocs.org/projects/tanbih-pipeline/badge/?version=latest
    :target: https://tanbih-pipeline.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

a flexible stream processing framework supporting RabbitMQ, Pulsar, Kafka and Redis.

Features
--------

- at-least-once guaranteed with acknowledgement on every message
- horizontally scalable through consumer groups
- flow is controlled in deployment, develop it once, use it everywhere
- testability provided with FILE and MEMORY input/output

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
    $ pip install tanbih-pipeline[azure]

If you want to support all backends, you can:

.. code-block:: bash

    $ pip install tanbih-pipeline[full]


Generator
---------

Generator is to be used when developing a data source in our pipeline. A source
will produce output without input. A crawler can be seen as a generator.

.. code-block:: python

    >>> from pipeline import Generator, Message
    >>>
    >>> class MyGenerator(Generator):
    ...     def generate(self):
    ...         for i in range(10):
    ...             yield {'id': i}
    >>>
    >>> generator = MyGenerator('generator', '0.1.0', description='simple generator')
    >>> generator.parse_args("--kind MEM --out-topic test".split())
    >>> generator.start()
    >>> [r.get('id') for r in generator.destination.results]
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


Processor
---------

Processor is to be used to process input. Modification will be in-place. A processor
can produce one output for each input, or no output.

.. code-block:: python

    >>> from pipeline import Processor, Message
    >>>
    >>> class MyProcessor(Processor):
    ...     def process(self, msg):
    ...         msg.update({'processed': True})
    ...         return None
    >>>
    >>> processor = MyProcessor('processor', '0.1.0', description='simple processor')
    >>> config = {'data': [{'id': 1}]}
    >>> processor.parse_args("--kind MEM --in-topic test --out-topic test".split(), config=config)
    >>> processor.start()
    >>> [r.get('id') for r in processor.destination.results]
    [1]


Splitter
--------

Splitter is to be used when writing to multiple outputs. It will take a function to
generate output topic based on the processing message, and use it when writing output.

.. code-block:: python

    >>> from pipeline import Splitter, Message
    >>>
    >>> class MySplitter(Splitter):
    ...     def get_topic(self, msg):
    ...         return '{}-{}'.format(self.destination.topic, msg.get('id'))
    ...
    ...     def process(self, msg):
    ...         msg.update({
    ...             'processed': True,
    ...         })
    ...         return None
    >>>
    >>> splitter = MySplitter('splitter', '0.1.0', description='simple splitter')
    >>> config = {'data': [{'id': 1}]}
    >>> splitter.parse_args("--kind MEM --in-topic test --out-topic test".split(), config=config)
    >>> splitter.start()
    >>> [r.get('id') for r in splitter.destinations['test-1'].results]
    [1]


Usage
-----

Writing a Worker
################


Choose Generator, Processor or Splitter to subclass from.

Environment Variables
*********************

Application accepts following environment variables:

+----------------+-----------------+---------------------+
|   environment  |  command line   |                     |
|   variable     |  argument       | options             |
+================+=================+=====================+
|   PIPELINE     |  --kind         | KAFKA, PULSAR, FILE |
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
|   INTOPIC      |  --in-topic     | topic to read       |
+----------------+-----------------+---------------------+
|   OUTTOPIC     |  --out-topic    | topic to write to   |
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
