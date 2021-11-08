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

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.7.3/wait /wait
RUN chmod +x /wait

WORKDIR /code
COPY test-splitter/* /code/
COPY dist /code

RUN python3 -m pip install "tanbih-pipeline[full]@file:///code/tanbih_pipeline-0.11.22-py3-none-any.whl"
ENV PYTHONPATH=/code
CMD /wait && python testsplitter.py
