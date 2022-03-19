ARG PYTHON_VERSION=3.9.4
FROM python:$PYTHON_VERSION

USER root
WORKDIR /opt
RUN wget -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz && \
    wget https://downloads.lightbend.com/scala/2.12.15/scala-2.12.15.tgz && \
    wget https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz && \
    wget https://github.com/sbt/sbt/releases/download/v1.3.0/sbt-1.3.0.tgz
RUN tar xzf jdk-8u131-linux-x64.tar.gz && \
    tar xvf scala-2.12.15.tgz && \
    tar xvf spark-3.2.1-bin-hadoop3.2.tgz && \
    tar xvf sbt-1.3.0.tgz
ENV PATH="/opt/jdk1.8.0_131/bin:/opt/scala-2.12.15/bin:/opt/spark-3.2.1-bin-hadoop3.2/bin:/opt/sbt/bin:/opt/sbt/bin$PATH"

WORKDIR /app


