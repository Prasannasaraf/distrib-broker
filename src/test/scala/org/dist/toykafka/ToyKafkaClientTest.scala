package org.dist.toykafka

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer

class ToyKafkaClientTest extends ZookeeperTestHarness {
  test("Toy Kafka should register brokers") {
    val toyKafkaClient = new ToyKafkaClient(zkClient);
    toyKafkaClient.registerBroker(Broker(0, "10.10.10.10", 8000))
    toyKafkaClient.registerBroker(Broker(1, "10.10.10.11", 8000))
    toyKafkaClient.registerBroker(Broker(2, "10.10.10.12", 8000))

    assert(3 == toyKafkaClient.getAllBrokers().size)
  }

  test("should get notified when broker is registered") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val toyKafkaClient1 = new ToyKafkaClient(zkClient1)
    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val toyKafkaClient2 = new ToyKafkaClient(zkClient2);
    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val toyKafkaClient3 = new ToyKafkaClient(zkClient3);

    val listener = new ToyKafkaBrokerChangeListener(toyKafkaClient1)
    toyKafkaClient1.subscribeBrokerChangeListener(listener);
    toyKafkaClient1.registerBroker(Broker(0, "10.10.10.10", 8000))

    toyKafkaClient2.registerBroker(Broker(1, "10.10.10.11", 8000))

    toyKafkaClient3.registerBroker(Broker(2, "10.10.10.12", 8000))


    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 10000)

    zkClient3.close()
    zkClient2.close()

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 1
    }, "Waiting for all brokers to get added", 10000)

  }
}
