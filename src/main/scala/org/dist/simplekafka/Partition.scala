package org.dist.simplekafka

import java.io._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import org.dist.simplekafka.api.{RequestKeys, RequestOrResponse}
import org.dist.simplekafka.common.{JsonSerDes, Logging, TopicAndPartition}
import org.dist.simplekafka.network.InetAddressAndPort
import org.dist.simplekafka.server.Config
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class Partition(config:Config, topicAndPartition: TopicAndPartition) (implicit system:ActorSystem) extends Logging {
  val LogFileSuffix = ".log"
  val logFile =
    new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + LogFileSuffix)

  val sequenceFile = new SequenceFile()
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)

  case class BrokerAndFetcherId(broker: Broker, fetcherId: Int)

  val numFetchers = 1
  private val fetcherThreadMap = new mutable.HashMap[BrokerAndFetcherId, ReplicaFetcherThread]
  private def getFetcherId(topicAndPartition: TopicAndPartition) : Int = {
    (topicAndPartition.topic.hashCode() + 31 * topicAndPartition.partition) //% numFetchers
  }

  def createFetcherThread(fetcherId: Int, leaderBroker: Broker): ReplicaFetcherThread = {
    new ReplicaFetcherThread("ReplicaFetcherThread-%d-%d".format(fetcherId, leaderBroker.id), leaderBroker, this, config)
  }

  def addFetcher(topicAndPartition: TopicAndPartition, initialOffset: Long, leaderBroker: Broker) {
      var fetcherThread: ReplicaFetcherThread = null
      val key = new BrokerAndFetcherId(leaderBroker, getFetcherId(topicAndPartition))
      fetcherThreadMap.get(key) match {
        case Some(f) => fetcherThread = f
        case None =>
          fetcherThread = createFetcherThread(key.fetcherId, leaderBroker)
          fetcherThreadMap.put(key, fetcherThread)
          fetcherThread.start
      }
      fetcherThread.addPartition(topicAndPartition, initialOffset)
      info("Adding fetcher for partition [%s,%d], initOffset %d to broker %d with fetcherId %d"
        .format(topicAndPartition.topic, topicAndPartition.partition, initialOffset, leaderBroker.id, key.fetcherId))
    }

  def makeFollower(leader:Broker) = {
    addFetcher(topicAndPartition, sequenceFile.lastOffset(), leader)
  }

  def makeLeader() = {
    //stop fetcher threads.
  }


  object PartitionTopicInfo {
    val InvalidOffset = -1L

    def isOffsetInvalid(offset: Long) = offset < 0L
  }

  class ReplicaFetcherThread(name:String,
                             leaderBroker: Broker,
                             parition:Partition, config:Config) extends Thread with Logging {

    var topicPartitions = new ListBuffer[TopicAndPartition]()

    def addPartition(topicAndPartition:TopicAndPartition, initialOffset: Long) {
        topicPartitions += topicAndPartition
    }

    val isRunning: AtomicBoolean = new AtomicBoolean(true)
    val correlationId = new AtomicInteger(0)
    val socketClient = new SocketClient
    def doWork(): Unit = {
      parition.sequenceFile.lastOffset();
      if (!topicPartitions.isEmpty) {
        val topicPartition = topicPartitions(0) //expect only only for now.
        val consumeRequest = ConsumeRequest(topicPartition, parition.sequenceFile.lastOffset() + 1, config.brokerId)
//        info(s"Fetching messages from offset ${parition.sequenceFile.lastOffset()} for topic partition ${topicPartition} in broker ${config.brokerId}")
        val request = RequestOrResponse(RequestKeys.FetchKey, JsonSerDes.serialize(consumeRequest), correlationId.getAndIncrement())
        val response = socketClient.sendReceiveTcp(request, InetAddressAndPort.create(leaderBroker.host, leaderBroker.port))
        val consumeResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[ConsumeResponse])
        consumeResponse.messages.foreach(m ⇒ {
          info(s"Replicating message ${m} for topic partition ${topicPartition} in broker ${config.brokerId}")
          parition.append(m._1, m._2)
        })
      }
    }

    override def run(): Unit = {
      info("Starting ")
      try{
        while(isRunning.get()){
          doWork()
        }
      } catch{
        case e: Throwable =>
          if(isRunning.get())
            error("Error due to ", e)
      }
      info("Stopped ")
    }
  }
  val source: Source[(String, String, Promise[Int]), ActorRef] = Source.actorRef(100, OverflowStrategy.dropHead)
  private val (actorRef, s) = source.preMaterialize()

  s.runForeach({ case (k, v, p) ⇒
    val index = append(k, v)
    p.success(index)
  })

  def append2(key: String, message: String) = {
    val p = Promise[Int]()
    actorRef ! (key, message, p)
    Await.result(p.future, 1.second)
  }

  def append(key:String, message:String) = {
    val currentPos = writer.getCurrentPosition
    try writer.append(key, message)
    catch {
      case e: IOException =>
        writer.seek(currentPos)
        throw e
    }
  }

  def read(offset:Long = 0) = {
    val result = new java.util.ArrayList[Row]()
    val offsets = sequenceFile.getAllOffSetsFrom(offset)
    offsets.foreach(offset ⇒ {
      val filePosition = sequenceFile.offsetIndexes.get(offset)

      val ba = new ByteArrayOutputStream()
      val baos = new DataOutputStream(ba)

      reader.seekToOffset(filePosition)
      reader.next(baos)

      val bais = new DataInputStream(new ByteArrayInputStream(ba.toByteArray))
      Try(Row.deserialize(bais)) match {
        case Success(row) => result.add(row)
        case Failure(exception) => None
      }
    })
    result.asScala.toList
  }


  object Row {
    def serialize(row: Row, dos:DataOutputStream): Unit = {
      dos.writeUTF(row.key)
      dos.writeInt(row.value.getBytes().size)
      dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
    }

    def deserialize(dis: DataInputStream): Row = {
      val key = dis.readUTF()
      val dataSize = dis.readInt()
      val bytes = new Array[Byte](dataSize)
      dis.read(bytes)
      val value = new String(bytes) //TODO:As of now supporting only string values
      Row(key, value)
    }
  }

  case class Row(key: String, value: String)
}
