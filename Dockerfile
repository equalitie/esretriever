# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


FROM openjdk:8
FROM python:3.6

ARG BRANCH

# Get jdk8 from previous stage https://docs.docker.com/develop/develop-images/multistage-build/
COPY --from=openjdk:8 /usr/local/openjdk-8 /usr/local/openjdk-8

# Set java path
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH $PATH:$JAVA_HOME/bin

RUN apt-get update

# pip >= 19 breaks the --process-dependency-links functionality
RUN mkdir app \
    && cd app \
    && pip install --upgrade pip==18.0 \
    && git clone -b $BRANCH  https://github.com/equalitie/esretriever.git \
    && cd esretriever/ && pip install . \
    && apt-get -y autoremove

RUN ["chmod", "u+rwx", "/app/esretriever/src/es_retriever/examples/simple_retrieve_cli.py"]

WORKDIR /app/esretriever/src/es_retriever/examples

EXPOSE 4040 4041