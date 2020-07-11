package org.dist.toykafka

import org.dist.simplekafka.{ControllerExistsException, SimpleSocketServer}

case class ToyControllerExistsException(existingControllerId: String) extends RuntimeException

class ToyController(val zookeeperClient: ToyZookeeperClient, val brokerId: Int) {
  var currentLeader = -1

  def elect() = {
    val leaderId = brokerId.toString
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId;
    } catch {
      case e: ToyControllerExistsException => {
        this.currentLeader = e.existingControllerId.toInt
      }
    }
  }

  def startup() = {
   zookeeperClient.subscribeControllerChangeListener(this)
    elect()
  }

  def setCurrent(brokerID: Nothing): Unit = {
    currentLeader = brokerID
  }
}
