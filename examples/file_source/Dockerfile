FROM python:3.6

RUN 'pip install pyconnect'

COPY . /app/

WORKDIR /app

ENV PYCONNECT_BOOTSTRAP_SERVERS = 'broker:9092'
ENV PYCONNECT_SCHEMA_REGISTRY = 'schema-registry:8082'
ENV PYCONNECT_TOPIC = 'testtopic-source'
ENV PYCONNECT_OFFSET_TOPIC = 'testtopic-source-offset'
ENV PYCONNECT_SOURCE_DIRECTORY = '/tmp/filesource/',
ENV PYCONNECT_SOURCE_FILENAME = 'sourcefile.json'

ENTRYPOINT ['python', 'file_source.py', '--config', 'env']