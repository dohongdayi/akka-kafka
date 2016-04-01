package io.github.junheng.akka.kafka.proxy

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import io.github.junheng.akka.accessor.access.{Accessor, AccessorAdapter}
import io.github.junheng.akka.kafka.{KGroup, KService, KTopic}
import io.github.junheng.akka.locator.{LoadMonitor, Service, ServiceLocator}
import io.github.junheng.akka.monitor.dispatcher.MonitoredForkJoinPool
import io.github.junheng.akka.monitor.mailbox.SafeMailboxMonitor
import org.apache.log4j.{Level, Logger}

import scala.language.postfixOps

object APP extends App {

  Logger.getRootLogger.setLevel(Level.OFF)

  implicit val system = ActorSystem("kafka")

  implicit val config = ConfigFactory.load()

  MonitoredForkJoinPool.logger(system.log)

  LoadMonitor.monitorActorRef = system.actorOf(Props(new SafeMailboxMonitor(config.getConfig("safe-mailbox-monitor"))), "safe-mailbox-monitor")

  ServiceLocator.initialize(config.getString("server-locator.zookeepers"), config.getString("server-locator.name"))

  Accessor.start(config.getString("accessor.host"), config.getInt("accessor.port"))

  KService.propsKService = config => Props(new KService(config) with Service with AccessorAdapter)

  KService.propsKTopic = (zookeepers, brokers, consumerCache, config) => Props(new KTopic(zookeepers, brokers, consumerCache, config) with Service with AccessorAdapter)

  KService.propsKGroup = (zookeepers, topic, group, pullerCount, cache) => Props(new KGroup(zookeepers, topic, group, pullerCount, cache) with Service with AccessorAdapter)

  KService.start(config.getConfig("kafka"))

}

