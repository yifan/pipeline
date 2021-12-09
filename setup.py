#!/usr/bin/env python
import codecs
import os

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), "requirements.txt")) as f:
    required = f.read().splitlines()

with codecs.open(
    os.path.join(os.path.dirname(__file__), "README.rst"), "r", "utf-8"
) as f:
    readme = f.read()

setup(
    name="tanbih-pipeline",
    version="0.11.31",
    description="a pipeline framework for streaming processing",
    entry_points={
        "console_scripts": {
            "pipeline-copy = pipeline.__main__:copy",
        },
    },
    long_description=readme,
    long_description_content_type="text/x-rst",
    author="yifan",
    author_email="yzhang@hbku.edu.qa",
    url="https://github.com/yifan/pipeline",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    packages=find_packages("./src"),
    package_dir={"": "src"},
    py_modules=[
        "pipeline",
    ],
    include_package_data=True,
    zip_safe=False,
    install_requires=required,
    extras_require={
        "full": [
            "redis",
            "confluent-kafka==1.3.*",
            "pulsar-client==2.5.*",
            "azure-cosmosdb-table",
            "pika",
            "pymongo",
        ],
        "redis": ["redis"],
        "kafka": ["confluent-kafka==1.3.*"],
        "pulsar": ["pulsar-client==2.5.*"],
        "mysql": ["mysql-connector-python"],
        "rabbitmq": ["pika"],
        "azure": ["azure-cosmosdb-table"],
        "mongodb": ["pymongo"],
    },
)
