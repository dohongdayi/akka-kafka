package io.github.junheng.akka.kafka

import akka.event.LoggingAdapter
import io.github.junheng.akka.kafka.protocol.KConsumerManagerProtocol.PartitionStatus
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{BrokerNotAvailableException, ErrorMapping, OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.network.BlockingChannel
import kafka.utils.{Json, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.common.security.JaasUtils

import scala.collection.{Map, Seq, immutable, mutable}
import scala.concurrent.{ExecutionContext, Future}

class KConsumerManager(topic: String, group: String, zookeeper: String, log: LoggingAdapter)(implicit execution: ExecutionContext) {
  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()
  private val offsetMap: mutable.Map[TopicAndPartition, Long] = mutable.Map()
  private var topicPidMap: immutable.Map[String, Seq[Int]] = immutable.Map()
  private val zkUtils: ZkUtils = ZkUtils(zookeeper,
    30000,
    30000,
    JaasUtils.isZkSecurityEnabled())

  private def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + bid)._1 match {
        case Some(brokerInfoString) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        println("Could not parse broker info due to " + t.getCause)
        None
    }
  }

  private def processPartition(group: String, topic: String, pid: Int): Option[PartitionStatus] = {
    val topicPartition = TopicAndPartition(topic, pid)
    val offsetOpt = offsetMap.get(topicPartition)
    val groupDirs = new ZKGroupTopicDirs(group, topic)
    val owner = zkUtils.readDataMaybeNull(groupDirs.consumerOwnerDir + "/%s".format(pid))._1
    zkUtils.getLeaderForPartition(topic, pid) match {
      case Some(bid) =>
        val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
        consumerOpt match {
          case Some(consumer) =>
            val topicAndPartition = TopicAndPartition(topic, pid)
            val request =
              OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
            val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
            val lagOpt = offsetOpt.map(o => if (o == -1) -1 else logSize - o)
            Some(PartitionStatus(group, topic, pid, offsetOpt.getOrElse(-1), logSize, lagOpt.getOrElse(-1), owner.getOrElse("none")))
          case None => None
        }
      case None => {
        println("No broker for partition %s - %s".format(topic, pid))
        None
      }
    }
  }

  private def processTopic(group: String, topic: String): Option[List[PartitionStatus]] = {
    topicPidMap.get(topic) map { pids =>
      pids.sorted.map(pid => processPartition(group, topic, pid)).filterNot(_.isEmpty).map(_.get).toList
    }
  }

  def getPartitionStatus: Future[List[PartitionStatus]] = Future {
    val channel: BlockingChannel = null
    try {
      val topicList = List(topic)
      topicPidMap = immutable.Map(zkUtils.getPartitionsForTopics(topicList).toSeq: _*)
      val topicPartitions = topicPidMap.flatMap { case (_topic, partitionSeq) => partitionSeq.map(TopicAndPartition(_topic, _)) }.toSeq
      val channel = ClientUtils.channelToOffsetManager(group, zkUtils)

      channel.send(OffsetFetchRequest(group, topicPartitions))
      val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload)

      offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
        if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
          val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
          // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
          // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
          try {
            val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/%d".format(topicAndPartition.partition))._1.toLong
            offsetMap.put(topicAndPartition, offset)
          } catch {
            case zkNoNodeException: ZkNoNodeException if zkUtils.pathExists(topicDirs.consumerOffsetDir) =>
              offsetMap.put(topicAndPartition, -1)
            case ex: Exception => throw ex
          }
        }
        else if (offsetAndMetadata.error == ErrorMapping.NoError)
          offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
        else {
          log.info("Could not fetch offset for %s due to %s.".format(topicAndPartition, ErrorMapping.exceptionFor(offsetAndMetadata.error)))
        }
      }
      channel.disconnect()

      val result = topicList.sorted.map(topic => processTopic(group, topic)).filterNot(_.isEmpty).map(_.get).foldLeft(List[PartitionStatus]())(_ ::: _)

      for ((_, consumerOpt) <- consumerMap)
        consumerOpt match {
          case Some(consumer) => consumer.close()
          case None => // ignore
        }
      result
    }
    finally {
      consumerMap.values.foreach {
        case Some(consumer) => consumer.close()
        case None =>
      }
      if (zkUtils.zkClient != null) zkUtils.zkClient.close()
      if (channel != null) channel.disconnect()
    }
  }

  def rewind(partitions: Int): Future[Unit] = Future {
    val partitionOffsets = 1 to partitions map (i => s"/consumers/$group/offsets/$topic/$i") map (k => k -> "0") toMap

    for ((partition, offset) <- partitionOffsets) {
      log.info("updating [" + partition + "] with offset [" + offset + "]")

      try {
        zkUtils.updatePersistentPath(partition, offset.toString)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
    }
    if (zkUtils.zkClient != null) zkUtils.zkClient.close()
  }
}
