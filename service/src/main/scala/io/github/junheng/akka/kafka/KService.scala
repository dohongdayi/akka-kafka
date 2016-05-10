package io.github.junheng.akka.kafka

import akka.actor._
import akka.cluster.singleton.{ClusterSingletonManagerSettings, ClusterSingletonManager, ClusterSingletonProxySettings, ClusterSingletonProxy}
import com.typesafe.config.Config
import scala.collection.JavaConversions._

import scala.collection.mutable

class KService(config: Config) extends Actor with ActorLogging {

  private implicit val execution = context.dispatcher

  private val topics = mutable.Map[String, ActorRef]()

  private val zookeepers = config.getString("zookeepers")
  private val brokers = config.getString("brokers")
  private val defaultCache = config.getInt("cache")

  override def preStart(): Unit = {
    config.getConfigList("topics") foreach { topicConfig =>
      val cache = if(topicConfig.hasPath("cache")) topicConfig.getInt("cache") else defaultCache
      val topicId = topicConfig.getString("id")
      topics += topicId -> context.actorOf(KService.propsKTopic(zookeepers, brokers, cache, topicConfig), topicId)
    }
    log.info("started")
  }

  override def receive: Receive = {
    case _ =>
  }
}


object KService {

  var propsKService: (Config) => Props = config => Props(new KService(config))

  var propsKTopic: (String, String, Int, Config) => Props = (zookeepers, brokers, consumerCache, config) => Props(new KTopic(zookeepers, brokers, consumerCache, config))

  var propsKGroup: (String, String, String, Int, Int) => Props = (zookeepers, topic, group, pullerCount, cache) => Props(new KGroup(zookeepers, topic, group, pullerCount, cache))

  def defaultOfKGroupProps(zookeepers: String, topic: String, group: String, pullerCount: Int, cache: Int): Props = {
    Props(new KGroup(zookeepers, topic, group, pullerCount, cache))
  }

  def defaultKTopicProps(zookeepers: String, brokers: String, consumerCache: Int, config: Config): Props = {
    Props(new KTopic(zookeepers, brokers, consumerCache, config))
  }

  def start(config: Config)(implicit factory: ActorRefFactory) = factory.actorOf(KService.propsKService(config), "kafka")

  case class GetConsumer(group: String)

}


