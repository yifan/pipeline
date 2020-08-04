#!/usr/bin/env python
import codecs
import os

from setuptools import find_packages, setup

with open(os.path.join(os.path.dirname(__file__), "requirements.txt")) as f:
    required = f.read().splitlines()

with codecs.open(os.path.join(os.path.dirname(__file__), "README.rst"), "r", "utf-8") as f:
    readme = f.read()

setup(name="tanbih-pipeline",
      version="0.7.0",
      description="a pipeline framework for streaming processing",
      long_description=readme,
      long_description_content_type='text/x-rst',
      author="yifan",
      author_email="yzhang@hbku.edu.qa",
      url="https://github.com/yifan/pipeline",
      license="MIT",
      classifiers=[
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 3",
          "Programming Language :: Python :: 3.7",
      ],
      packages=find_packages("./src"),
      package_dir={"": "src"},
      py_modules=["pipeline", ],
      include_package_data=True,
      zip_safe=False,
      install_requires=required,
      )
