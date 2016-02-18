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
  private val consumerCache = config.getInt("consumer-cache")


  override def preStart(): Unit = {
    config.getConfigList("topics") foreach { topicConfig =>
      val topicId = topicConfig.getString("id")
      topics += topicId -> context.actorOf(KService.propsKTopic(zookeepers, brokers, consumerCache, topicConfig), topicId)
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

  def topic(topic: String)(implicit context: ActorContext) = {
    context.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/kafka/$topic",
        settings = ClusterSingletonProxySettings(context.system).withRole("Kafka")
      ),
      s"$topic-proxy"
    )
  }

  def group(topic: String, group: String)(implicit context: ActorContext) = {
    context.actorOf(
      ClusterSingletonProxy.props(
        singletonManagerPath = s"/user/kafka/$topic/singleton/$group",
        settings = ClusterSingletonProxySettings(context.system).withRole("Kafka")
      ),
      s"$topic-$group-proxy"
    )
  }

  def startOnCluster(config: Config)(implicit system: ActorSystem) = {
    val defaultPropsKTopic = propsKTopic
    val defaultPropsKGroup = propsKGroup
    propsKTopic = (zookeepers, brokers, consumerCache, topicConfig) => {
      ClusterSingletonManager.props(
        singletonProps = defaultPropsKTopic(zookeepers, brokers, consumerCache, topicConfig),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole("Kafka")
      )
    }
    propsKGroup = (zookeepers, topic, group, pullerCount, cache) => {
      ClusterSingletonManager.props(
        singletonProps = defaultPropsKGroup(zookeepers, topic, group, pullerCount, cache),
        terminationMessage = PoisonPill,
        settings = ClusterSingletonManagerSettings(system).withRole("Kafka")
      )
    }
    start(config)
  }

  def defaultOfKGroupProps(zookeepers: String, topic: String, group: String, pullerCount: Int, cache: Int): Props = {
    Props(new KGroup(zookeepers, topic, group, pullerCount, cache))
  }

  def defaultKTopicProps(zookeepers: String, brokers: String, consumerCache: Int, config: Config): Props = {
    Props(new KTopic(zookeepers, brokers, consumerCache, config))
  }

  def start(config: Config)(implicit factory: ActorRefFactory) = factory.actorOf(KService.propsKService(config), "kafka")

  case class GetConsumer(group: String)

}


