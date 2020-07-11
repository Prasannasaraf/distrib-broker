package org.dist.toykafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

import scala.collection.JavaConverters._
import scala.collection.mutable

class ToyKafkaBrokerChangeListener(toyKafkaClient: ToyKafkaClient) extends IZkChildListener {

  var liveBrokers = Set.empty[Broker]

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    println(s"size of currentChilds is ${currentChilds.size}")
    val currentBrokers = currentChilds
      .asScala
      .map(_.toInt)
      .toSet

    val newBrokersIds: Set[Int] = currentBrokers -- liveBrokers.map(broker => broker.id)
    liveBrokers = currentBrokers.map(id => toyKafkaClient.getBrokerInfo(id))
  }
}
