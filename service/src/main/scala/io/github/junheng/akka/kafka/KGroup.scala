package io.github.junheng.akka.kafka

import akka.actor.{ActorLogging, Actor}
import io.github.junheng.akka.kafka.KConsumerBaseband.Configuration
import io.github.junheng.akka.kafka.KConsumerManager.PartitionStatus
import io.github.junheng.akka.kafka.KGroup._
import akka.pattern._

import scala.concurrent.duration._
import scala.language.postfixOps

class KGroup(zookeepers: String, topic: String, group: String, pullerCount: Int, cache: Int) extends Actor with ActorLogging {

  import context.dispatcher

  private val baseband = new KConsumerBaseband(Configuration(zookeepers, topic, group, pullerCount, cache), log)

  private val manager = new KConsumerManager(topic, group, zookeepers, log)

  override def preStart(): Unit = {
    baseband.start()
    log.info("started")
  }

  override def postStop(): Unit = {
    baseband.stop()
  }

  override def receive: Actor.Receive = {
    case Pull(amount) =>
      val pulled = baseband.pull(amount)
      if (pulled.isEmpty) context.system.scheduler.scheduleOnce(1 seconds, sender(), Pulled(topic, group, pulled))
      else sender() ! Pulled(topic, group, pulled)
    case GetStatus => pipe(manager.getPartitionStatus map sumStatuses) to sender()

    case Rewind =>
      val receipt = sender()
      manager.rewind(pullerCount) onSuccess {
        case _ => pipe(manager.getPartitionStatus map sumStatuses) to receipt
      }
      log.warning(s"$topic-$group was rewind, this will cause group offset reset!!")
  }

  def sumStatuses(statuses: List[PartitionStatus]): KGroupStatus = {
    KGroupStatus(
      topic,
      group,
      statuses.length,
      statuses.map(_.logSize).sum,
      statuses.map(_.offset).sum,
      statuses.map(_.lag).sum,
      statuses.sortBy(_.pid)
    )
  }
}

object KGroup {

  case object GetStatus

  case class KGroupStatus(topic: String, group: String, partitions: Int, logSize: Long, offset: Long, lag: Long, detail: List[PartitionStatus])

  case object Rewind


  case class Pull(amount: Int)

  case class Pulled(topic: String, group: String, payloads: List[Array[Byte]])

}
