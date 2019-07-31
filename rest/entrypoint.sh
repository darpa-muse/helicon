#!/usr/bin/env bash
#  vim:ts=4:sts=4:sw=4:et
#
#  Author: Hari Sekhon
#  Date: 2016-04-24 21:29:46 +0100 (Sun, 24 Apr 2016)
#
#  https://github.com/harisekhon/Dockerfiles
#
#  License: see accompanying Hari Sekhon LICENSE file
#
#  If you're using my code you're welcome to connect with me on LinkedIn and optionally send me feedback
#
#  https://www.linkedin.com/in/harisekhon
#

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

srcdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export JAVA_HOME="${JAVA_HOME:-/usr}"

export PATH="$PATH:/hadoop/sbin:/hadoop/bin"

if [ $# -gt 0 ]; then
    exec $@
else
    if ! [ -f /root/.ssh/authorized_keys ]; then
        ssh-keygen -t rsa -b 1024 -f /root/.ssh/id_rsa -N ""
        cp -v /root/.ssh/{id_rsa.pub,authorized_keys}
        chmod -v 0400 /root/.ssh/authorized_keys
    fi

    if ! [ -f /etc/ssh/ssh_host_rsa_key ]; then
        /usr/sbin/sshd-keygen || :
    fi

    if ! pgrep -x sshd &>/dev/null; then
        /usr/sbin/sshd
    fi
    echo
    SECONDS=0
    while true; do
        if ssh-keyscan localhost 2>&1 | grep -q OpenSSH; then
            echo "SSH is ready to rock"
            break
        fi
        if [ "$SECONDS" -gt 20 ]; then
            echo "FAILED: SSH failed to come up after 20 secs"
            exit 1
        fi
        echo "waiting for SSH to come up"
        sleep 1
    done
    echo
    if ! [ -f /root/.ssh/known_hosts ]; then
        ssh-keyscan localhost || :
        ssh-keyscan 0.0.0.0   || :
    fi | tee -a /root/.ssh/known_hosts
    hostname=$(hostname -f)
    if ! grep -q "$hostname" /root/.ssh/known_hosts; then
        ssh-keyscan $hostname || :
    fi | tee -a /root/.ssh/known_hosts

    mkdir -pv /hadoop/logs

    sed -i "s/localhost/$hostname/" /hadoop/etc/hadoop/core-site.xml

    ##
    # Add accumulo endpoint stuff
    sed "s/HOSTNAME/$hostname/g" /usr/local/accumulo/conf/accumulo-site-template.xml > /usr/local/accumulo/conf/accumulo-site.xml

    echo $hostname > /usr/local/accumulo/conf/gc
    echo $hostname > /usr/local/accumulo/conf/masters
    echo $hostname > /usr/local/accumulo/conf/monitor
    echo $hostname > /usr/local/accumulo/conf/slaves
    echo $hostname > /usr/local/accumulo/conf/tracers


    $ZOOKEEPER_HOME/bin/zkServer.sh start
    start-dfs.sh
    hdfs dfsadmin -safemode wait
    start-yarn.sh

    # TODO: only init if needed
    accumulo init --instance-name accumulo --user root --password secret

    $ACCUMULO_HOME/bin/start-all.sh

    if [ -d "/ingest" ]; then
        cd /ingest
        hdfs dfs -mkdir /ingest
        for f in `ls`
        do
           echo $f
           hdfs dfs -copyFromLocal $f /ingest/$f
           accumulo shell -u root -p secret -e "importtable $f /ingest/$f"
        done
    fi
    cd / ; java -jar musebrowser-jar-with-dependencies.jar
    tail -f /dev/null /hadoop/logs/*
    stop-yarn.sh
    stop-dfs.sh
fi
