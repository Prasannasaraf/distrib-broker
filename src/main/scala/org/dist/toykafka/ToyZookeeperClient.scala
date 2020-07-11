package org.dist.toykafka

import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkDataListener, ZkClient}

class ToyZookeeperClient(zkClient: ZkClient) {

  val ControllerPath = "/controller"

  def subscribeControllerChangeListener(controller: ToyController) = {
    zkClient.subscribeDataChanges(ControllerPath, new ToyControllerChangeListener(controller))
  }

  def tryCreatingControllerPath(leaderId: String) = {
    try {
      try {
        zkClient.createEphemeral(ControllerPath, leaderId)
      } catch {
        case e: ZkNoNodeException =>
          zkClient.createPersistent(ControllerPath)
          zkClient.createEphemeral(ControllerPath, leaderId)
      }
    } catch {
      case e: ZkNodeExistsException => {
        println(s"Yeah got node exists exception for $leaderId")
        val existingControllerId: String = zkClient.readData(ControllerPath)
        throw  ToyControllerExistsException(existingControllerId)
      }
    }
  }


  class ToyControllerChangeListener(controller: ToyController) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Any): Unit = {
      println(s"handleDataChange for $data")
      val brokerID = zkClient.readData(dataPath)
      controller.setCurrent(brokerID)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      println(s"Node deleted election starting from this node ${controller.brokerId}")
      controller.elect()
    }
  }

}
