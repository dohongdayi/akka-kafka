package io.github.junheng.akka.kafka

import java.util.Properties
import java.util.concurrent._

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.Config
import io.github.junheng.akka.kafka.protocol.KTopicProtocol.{BatchPayload, Payload}
import org.apache.kafka.clients.producer.{RecordMetadata, KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

import scala.collection.JavaConversions._
import scala.concurrent.{ExecutionContext, Future}

class KTopic(zookeepers: String, brokers: String, consumerCache: Int, config: Config) extends Actor with ActorLogging {
  type KRecord = ProducerRecord[Array[Byte], Array[Byte]]

  private implicit val executor =
    ExecutionContext.fromExecutor(
      new ThreadPoolExecutor(8, 64, 30, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](1000), new RejectedExecutionHandler {
        override def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor): Unit = {
          log.warning("thread queue was full")
          executor.getQueue.put(r)
        } //block call until available
      })
    )

  private val producer = new KafkaProducer[Array[Byte], Array[Byte]](configuration)

  private val topicId = config.getString("id")

  override def preStart(): Unit = {
    val partitions = if (config.hasPath("partition")) config.getInt("partition") else 0
    if (config.hasPath("groups")) {
      config.getStringList("groups") foreach { groupId =>
        context.actorOf(KService.propsKGroup(zookeepers, topicId, groupId, partitions, consumerCache), groupId)
      }
    }
    log.info("started")
  }

  override def receive: Actor.Receive = {
    case BatchPayload(payloads) => Future(payloads.foreach(p => send(p)))
    case Payload(key, content) => Future(producer.send(new KRecord(topicId, key, content)))
  }


  def send(p: Payload) = {
    producer.send(new ProducerRecord(topicId, p.key, p.content))
  }

  override def unhandled(message: Any): Unit = log.warning(s"unexpected message ${message.getClass.getCanonicalName}")

  def configuration = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "128000")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props
  }
}