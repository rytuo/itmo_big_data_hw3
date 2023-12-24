FROM ubuntu:latest AS build_base

RUN apt-get update -y \
    && apt-get install -y openjdk-8-jdk \
    && apt-get install -y lsb-release \
    && apt-get install -y net-tools \
    && apt-get install -y curl

RUN DEBIAN_FRONTEND="noninteractive" \
    TZ="Europe/Moscow" \
    apt-get install -y tzdata

RUN ln -sf /usr/share/zoneinfo/Europe/Moscow /etc/localtime \
    && dpkg-reconfigure --frontend noninteractive tzdata


FROM build_base AS build_hadoop_base

WORKDIR /opt

RUN curl -o hadoop-3.3.6.tar.gz https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xzf hadoop-3.3.6.tar.gz -C /opt
RUN rm hadoop-3.3.6.tar.gz
RUN ln -sf /opt/hadoop-3.3.6 /opt/hadoop

RUN apt-get update -y \
    && apt-get install -y python3-pip

RUN mkdir -p /tmp/hadoop-hadoop

WORKDIR /opt/hadoop

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# fix WARN NativeCodeLoader: Unable to load native-hadoop library for your platform...
ENV LD_LIBRARY_PATH=$HADOOP_HOME/lib/native
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop


FROM build_hadoop_base AS build_hadoop
