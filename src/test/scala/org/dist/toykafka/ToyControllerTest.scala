package org.dist.toykafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer

class ToyControllerTest extends ZookeeperTestHarness {

  test("Should elect first server as controller and get all live brokers") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client1 = new ToyZookeeperClient(zkClient1)
    val controller1 = new ToyController(client1, 1)
    controller1.startup()

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client2 = new ToyZookeeperClient(zkClient2)
    val controller2 = new ToyController(client2, 2)
    controller2.startup()

    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val client3 = new ToyZookeeperClient(zkClient3)
    val controller3 = new ToyController(client3, 3)
    controller3.startup()


    assert(controller1.currentLeader == 1)
    assert(controller2.currentLeader == 1)
    assert(controller3.currentLeader == 1)

    zkClient2.close()
    zkClient1.close()

    Thread.sleep(10000)

    TestUtils.waitUntilTrue(() => {
      controller2.currentLeader == 3
    }, "Waiting for all brokers to get added", 10000)

    assert(controller2.currentLeader == 2)
  }
}
