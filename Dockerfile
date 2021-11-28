ARG PYTHON_VERSION=3.9.4
FROM python:$PYTHON_VERSION

USER root
WORKDIR /opt
#RUN wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.11%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
RUN wget -c --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u131-b11/d54c1d3a095b4ff2b6607d096fa80163/jdk-8u131-linux-x64.tar.gz && \
    wget https://downloads.lightbend.com/scala/2.13.5/scala-2.13.5.tgz && \
    wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop3.2.tgz && \
    wget https://github.com/sbt/sbt/releases/download/v1.3.0/sbt-1.3.0.tgz
#RUN tar xzf OpenJDK11U-jdk_x64_linux_hotspot_11.0.11_9.tar.gz && \
RUN tar xzf jdk-8u131-linux-x64.tar.gz && \
    tar xvf scala-2.13.5.tgz && \
    tar xvf spark-3.1.1-bin-hadoop3.2.tgz && \
    tar xvf sbt-1.3.0.tgz
#ENV PATH="/opt/jdk-11.0.11+9/bin:/opt/scala-2.13.5/bin:/opt/spark-3.1.1-bin-hadoop3.2/bin:/opt/sbt/bin:/opt/sbt/bin$PATH"
ENV PATH="/opt/jdk1.8.0_131/bin:/opt/scala-2.13.5/bin:/opt/spark-3.1.1-bin-hadoop3.2/bin:/opt/sbt/bin:/opt/sbt/bin$PATH"


#TODO : Change the user to non root user
#USER 185
WORKDIR /app


