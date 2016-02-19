package io.github.junheng.akka.kafka.protocol

object KConsumerManagerProtocol {
  case class PartitionStatus(group: String, topic: String, pid: Int, offset: Long, logSize: Long, lag: Long, owner: String)

}
