FROM harisekhon/hadoop:2.9

USER root

ENV HADOOP_PREFIX /hadoop

ENV PATH $PATH:$HADOOP_PREFIX/bin

RUN chown -R root:root $HADOOP_PREFIX

RUN echo -e "\n* soft nofile 65536\n* hard nofile 65536" >> /etc/security/limits.conf

RUN curl -s http://mirror.reverse.net/pub/apache/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz | tar -xz -C /usr/local
RUN ln -s /usr/local/zookeeper-3.4.13 /usr/local/zookeeper;\
 chown -R root:root /usr/local/zookeeper-3.4.13;\
 mkdir -p /var/zookeeper
ENV ZOOKEEPER_HOME /usr/local/zookeeper
ENV PATH $PATH:$ZOOKEEPER_HOME/bin
ADD zookeeper/* $ZOOKEEPER_HOME/conf/

RUN curl -s http://archive.apache.org/dist/accumulo/1.9.2/accumulo-1.9.2-bin.tar.gz | tar -xz -C /usr/local
RUN ln -s /usr/local/accumulo-1.9.2 /usr/local/accumulo;\
 chown -R root:root /usr/local/accumulo-1.9.2
ENV ACCUMULO_HOME /usr/local/accumulo
ENV PATH $PATH:$ACCUMULO_HOME/bin
ADD accumulo/* $ACCUMULO_HOME/conf/

ADD entrypoint.sh /
RUN chown root:root /entrypoint.sh; chmod 700 /entrypoint.sh

RUN yum install sudo -y

ADD target/musebrowser-jar-with-dependencies.jar /

EXPOSE 2181 9000 50095 50096
