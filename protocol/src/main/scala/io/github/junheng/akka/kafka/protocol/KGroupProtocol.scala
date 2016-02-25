package io.github.junheng.akka.kafka.protocol

import io.github.junheng.akka.kafka.protocol.KConsumerManagerProtocol.PartitionStatus

object KGroupProtocol {

  case object GetStatus

  case class KGroupStatus(topic: String, group: String, partitions: Int, logSize: Long, offset: Long, lag: Long, detail: List[PartitionStatus])

  case object Rewind


  case class Pull(amount: Int)

  case class Pulled(topic: String, group: String, payloads: List[Array[Byte]]) extends KafkaPayloadMessage


}
