FROM ubuntu:20.04

RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata
RUN apt-get install -y git cmake wget python3 python3-pip
RUN python3 -m pip install -U pip

# python requirements
ADD deploy/requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt

# app
COPY . /usr/src/ton-indexer
WORKDIR /usr/src/ton-indexer

ARG TON_CONFIG_FILE
COPY ${TON_CONFIG_FILE} liteserver_config.json

ENV C_FORCE_ROOT 1

# entrypoint
ENTRYPOINT ["celery", "-A", "indexer", "worker", "--loglevel=INFO"]