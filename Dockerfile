ARG OPENJDK_VERSION=8u312-b07-jdk
FROM eclipse-temurin:$OPENJDK_VERSION

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends wget \
    && rm -rf /var/lib/apt/lists/*

USER root
WORKDIR /opt
RUN wget https://downloads.lightbend.com/scala/2.13.5/scala-2.13.5.tgz && \
    wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    wget https://github.com/sbt/sbt/releases/download/v1.3.0/sbt-1.3.0.tgz
RUN tar xvf scala-2.13.5.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz && \
    tar xvf sbt-1.3.0.tgz
ENV PATH="/opt/scala-2.13.5/bin:/opt/spark-3.1.1-bin-hadoop3.2/bin:/opt/sbt/bin:${PATH}"

WORKDIR /app
