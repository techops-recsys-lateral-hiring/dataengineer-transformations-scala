FROM sbtscala/scala-sbt:eclipse-temurin-11.0.15_1.7.1_2.12.16 AS build

#ENV SBT_VERSION 1.7.1
USER root
WORKDIR /opt
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/*
RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar xvf spark-3.3.0-bin-hadoop3.tgz
ENV PATH="/opt/spark-3.3.0-bin-hadoop3/bin:$PATH"

#TODO : Change the user to non root user
#USER 185
WORKDIR /app
ENTRYPOINT ["tail", "-f", "/dev/null"]
