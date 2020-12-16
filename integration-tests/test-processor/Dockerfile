FROM python:3.6.8-slim
MAINTAINER yifan

ENV LC_ALL=C.UTF-8 LANG=C.UTF-8 PIPENV_HIDE_EMOJIS=1
WORKDIR /code
RUN apt-get update \
    && apt-get install -y \
            librdkafka-dev \
            libxml2-dev \
            libxslt-dev \
            libwebp-dev \
            libmariadb-dev  \
            gcc \
    && rm -rf /var/cache/apk/*

RUN python3 -m ensurepip \
    && rm -rf /usr/lib/python*/ensurepip \
    && python3 -m pip install -U pip \
    && ln -sf $(which python) /bin/python

ENV LC_ALL=C.UTF-8 LANG=C.UTF-8 PIPENV_HIDE_EMOJIS=1

WORKDIR /code
COPY test-processor/* /code/
COPY dist/*.whl /code/

RUN python3 -m pip install "tanbih-pipeline[redis]@file:///code/tanbih_pipeline-0.8.4-py3-none-any.whl"
ENV PYTHONPATH=/code
CMD ["python", "testprocessor.py"]
