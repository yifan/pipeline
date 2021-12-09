Scripts
=======

pipeline-copy
*************

Installation
------------

The preferred way to use `pipeline-copy` is to use it in a project virtual environment
due to `pipeline`'s dependences. I use `poetry` in this case:

.. code-block:: shell

    poetry add 'tanbih-pipeline[full]'

or if you know you are only going to use it with certain backend technology, for example,
mongodb:

.. code-block:: shell

    poetry add 'tanbih-pipeline[mongodb]'


Usage
-----

.. code-block:: shell

    # dump jsonl to mongodb
    poetry run pipeline-copy -model-definition [./model.py] \
        --in-kind FILE --in-content-only \
        --in-filename [input.jsonl] \
        --out-kind MONGO \
        --out-uri [mongodb_uri] \
        --out-database [database] \
        --out-keyname key1,key2,key3 \
        --out-topic [collection]


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
