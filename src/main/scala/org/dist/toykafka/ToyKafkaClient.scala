package org.dist.toykafka

import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.dist.simplekafka.common.JsonSerDes

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

class ToyKafkaClient(zkClient: ZkClient) {
  val IdsPath = "/brokers/ids"
  def subscribeBrokerChangeListener(listener: ToyKafkaBrokerChangeListener) = {
    zkClient.subscribeChildChanges(IdsPath, listener)
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    println(s"getBrokerInfo started for $brokerId")
    val data: String = zkClient.readData(getBrokerPath(brokerId))
    val result = JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    println(s"getBrokerInfo finished for $brokerId")
    result
  }

  def getBrokerPath(id: Int) = {
    IdsPath + "/" + id
  }

  def registerBroker(broker: Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    try {
      zkClient.createEphemeral(getBrokerPath(broker.id), brokerData)
    } catch {
      case e: ZkNoNodeException =>
        zkClient.createPersistent(IdsPath, true)
        zkClient.createEphemeral(getBrokerPath(broker.id), brokerData)
    }
  }

  def getAllBrokers() = {
    zkClient.getChildren(IdsPath).asScala.toList
  }
}
