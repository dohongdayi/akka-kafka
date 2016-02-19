package io.github.junheng.akka.kafka.proxy

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.github.junheng.akka.accessor.access.Accessor
import io.github.junheng.akka.kafka.{KGroup, KService, KTopic}
import io.github.junheng.akka.locator.{Service, ServiceLocator}
import io.github.junheng.akka.monitor.dispatcher.MonitoredForkJoinPool
import io.github.junheng.akka.monitor.mailbox.SafeMailboxMonitor
import org.apache.log4j.{Level, Logger}

object APP extends App {

  Logger.getRootLogger.setLevel(Level.OFF)

  implicit val system = ActorSystem("proxy")

  implicit val config = ConfigFactory.load()

  MonitoredForkJoinPool.logger(system.log)

  system.actorOf(Props(new SafeMailboxMonitor(config.getConfig("safe-mailbox-monitor"))), "safe-mailbox-monitor")

  ServiceLocator.initialize("phb01,phb02,phb03")

  Accessor.start(config.getString("accessor.host"), config.getInt("accessor.port"))

  KService.propsKService = config => Props(new KService(config) with Service)

  KService.propsKTopic = (zookeepers, brokers, consumerCache, config) => Props(new KTopic(zookeepers, brokers, consumerCache, config) with Service)

  KService.propsKGroup = (zookeepers, topic, group, pullerCount, cache) => Props(new KGroup(zookeepers, topic, group, pullerCount, cache) with Service)

  KService.start(config.getConfig("kafka"))

}