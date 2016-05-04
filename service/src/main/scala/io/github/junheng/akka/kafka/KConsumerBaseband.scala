package io.github.junheng.akka.kafka

import java.util.Properties
import java.util.concurrent.{Executors, LinkedBlockingQueue}

import akka.event.LoggingAdapter
import io.github.junheng.akka.kafka.KConsumerBaseband.Status.Status
import io.github.junheng.akka.kafka.KConsumerBaseband.{Status, Configuration}
import io.github.junheng.akka.kafka.KConsumerBaseband.Status.Status
import kafka.consumer.{ConsumerConfig, Consumer, ConsumerTimeoutException}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future}


class KConsumerBaseband(props: Configuration, log: LoggingAdapter) {

  import props._

  private implicit val execution = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(pullerCount + 1))

  private val consumer = Consumer.create(configuration(group))

  private val topics = Map(topic -> pullerCount)

  private val streams = consumer.createMessageStreams(topics)(topic)

  private val buffer = new LinkedBlockingQueue[Array[Byte]](cache)

  private var _status: Status = Status.NotStarted

  private var isConsumeContinue: Boolean = false

  def start() = {
    isConsumeContinue = true
    Future {
      while (isConsumeContinue) {
        log.info(s"[$topic - $group] CACHE [${buffer.size()}] status [${_status}]")
        Thread.sleep(60000)
      }
    }
    streams foreach { stream =>
      Future {
        log.info(s"started stream consumer [${Thread.currentThread().getName}]")
        val iterator = stream.iterator()
        while (isConsumeContinue) {
          try {
            if (iterator.hasNext()) {
              val message: Array[Byte] = iterator.next().message()
              buffer.put(message)
              _status = Status.Fetching
            }
          } catch {
            case ex: ConsumerTimeoutException => log.debug("fetch timeout..., usually no more data, re-try now")
            case ex: java.util.NoSuchElementException =>
              log.info(s"no more data in this partition wait $waitTimeInEOF ms, and try again")
              _status =Status. NoMoreDataCurrently
              Thread.sleep(waitTimeInEOF)
            case ex: Exception =>
              log.error(ex, s"stop waiting data from kafka cause ${ex.getMessage}, sleep $waitTimeInUnexpectedError ms")
              _status = Status.MeetUnexpectedErrorCurrently
              Thread.sleep(waitTimeInUnexpectedError)
          }
        }
        log.info(s"stopped stream consumer [${Thread.currentThread().getName}]")
      }
    }
  }

  def stop() = {
    isConsumeContinue = false
    consumer.shutdown()
    _status = Status.Stopped
  }

  def status() = _status

  def pull(amount: Int): List[Array[Byte]] = {
    val result = ArrayBuffer[Array[Byte]]()
    while (buffer.size() > 0 && result.size < amount) {
      buffer.poll match {
        case null => //do nothing if data not correct
        case data => result += data
      }
    }
    result.toList
  }

  def configuration(group: String) = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("group.id", group)
    props.put("rebalance.max.retries", "30")
    props.put("rebalance.backoff.ms", "5000")
    props.put("zookeeper.session.timeout.ms", zkSessionTimeout)
    props.put("zookeeper.sync.time.ms", zkSyncTime)
    props.put("auto.commit.interval.ms", commitInterval)
    props.put("auto.offset.reset", autoOffsetReset)
    props.put("consumer.timeout.ms", "15000")
    props.put("fetch.message.max.bytes", "16777216")
    props.put("partition.assignment.strategy", "roundrobin")
    new ConsumerConfig(props)
  }
}

object KConsumerBaseband {

  case class Configuration
  (
    zookeepers: String,
    topic: String,
    group: String,
    pullerCount: Int,
    cache: Int = 100000,
    waitTimeInEOF: Long = 500,
    waitTimeInUnexpectedError: Long = 1000,
    zkSessionTimeout: String = "10000",
    zkSyncTime: String = "200",
    commitInterval: String = "1000",
    autoOffsetReset: String = "smallest"
  )

  case object Status extends Enumeration {
    type Status = Value
    val Fetching, NotStarted, Stopped, NoMoreDataCurrently, MeetUnexpectedErrorCurrently = Value
  }
}
