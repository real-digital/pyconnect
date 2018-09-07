FROM python:3.6

RUN ["pip", "install", "pyconnect"]

COPY . /app/

WORKDIR /app

ENV PYCONNECT_BOOTSTRAP_SERVERS = 'broker:9092'
ENV PYCONNECT_SCHEMA_REGISTRY = 'schema-registry:8082'
ENV PYCONNECT_TOPICS = 'testtopic-sink'
ENV PYCONNECT_SINK_DIRECTORY = '/tmp/filesink/'
ENV PYCONNECT_FILENAME = 'sinkfile.json'
ENV PYCONNECT_GROUP_ID = 'file_sink_test'

ENTRYPOINT ["python", "file_sink.py", "--config", "env"]