FROM openjdk:8
FROM python:3.6

ARG ELK_USER
ARG ELK_PASSWORD
ARG BRANCH

# Get jdk8 from previous stage https://docs.docker.com/develop/develop-images/multistage-build/
COPY --from=openjdk:8 /usr/local/openjdk-8 /usr/local/openjdk-8

# Set java path
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH $PATH:$JAVA_HOME/bin

RUN apt-get update # && apt-get -y install software-properties-common

RUN mkdir app && cd app

RUN git clone -b $BRANCH  hhttps://github.com/equalitie/esretriever.git \
RUN cd /app/esretriever/ && pip install . && apt-get -y autoremove

RUN ["chmod", "u+rwx", "app/esretriever/src/es_retriever/examples/simple_retrieve_cli.py"]

WORKDIR /app/esretriever/src/es_retriever/examples

EXPOSE 4040 4041