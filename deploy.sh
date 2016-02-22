#!/bin/sh

if [ $# -ne 1 ];then
echo "./deploy.sh hostList"
exit 1;
fi

rsync -vzrtopg --progress --delete proxy/target/universal/stage/lib/ -e ssh deploy@pcl01:/app/share/deploy/kafka/lib/
rsync -vzrtopg --progress --delete proxy/target/universal/stage/bin/ -e ssh deploy@pcl01:/app/share/deploy/kafka/bin/

sleep 10

hostList=$1
hosts=${hostList//,/ }

for host in ${hosts}
do
echo ${host}
ssh -n deploy@${host} "ps -ef | grep akka-kafka-proxy |grep java| grep -v grep | awk '{print \$2}'| xargs kill ||echo start"
sleep 5
ssh -n deploy@${host}  "rm akka-kafka.log"
ssh -n deploy@${host}  "/app/share/deploy/kafka/bin/akka-kafka-proxy -J-Dhost=${host} -J-Xms4G -J-Xmx4G -J-XX:+UseParNewGC -J-XX:+UseConcMarkSweepGC -J-XX:-CMSConcurrentMTEnabled -J-XX:CMSInitiatingOccupancyFraction=65 -J-XX:+CMSParallelRemarkEnabled -J-XX:+HeapDumpOnOutOfMemoryError -J-XX:HeapDumpPath=dump.heap > akka-kafka.log &"

done