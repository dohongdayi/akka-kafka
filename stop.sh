#!/bin/sh

if [ $# -ne 1 ];then
echo "./stop.sh hostList"
exit 1;
fi

hostList=$1
hosts=${hostList//,/ }

for host in ${hosts}
do
echo ${host}
ssh -n deploy@${host} "ps -ef | grep akka-kafka-proxy |grep java| grep -v grep | awk '{print \$2}'| xargs kill ||echo start"
done