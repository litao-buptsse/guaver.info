# Kafka源码解析(一) Producer

---

最近在做Kafka相关工作，本篇重点梳理Kafka Producer源码逻辑。

## Producer整体架构

Producer端架构很简单，当客户端调用send()方法时，根据配置的sync.type分两路执行。若为sync模式，则直接调用EventHandler.handle()发送消息；若为async模式，则先将message放入queue，再由ProducerThread定期从queue获取消息，最终仍调用EventHandler.handle()发送消息。下面分别对每个流程进行讨论。

![kafka-producer-pic1](http://7xid4y.com1.z0.glb.clouddn.com/kafka-producer-pic1.png)

## Queue & ProducerThread

这部分很简单，主要有如下参数可以配置:

    queue.buffering.max.message：队列容量，默认10000
    queue.buffering.max.ms：消息在队列的最大缓存时间，默认5000
    queue.enqueue.timeout.ms：默认0
      0： 对应queue.offer(msg) // 若queue满，即可丢弃消息
      -v：对应queue.put(msg) // 若queue满，阻塞
      +v：对应queue.offer(msg, timeout) // 若queue满，阻塞直至超时
    batch.num.messages：批量发送消息的数量，默认200
    
## DefaultEventHandler基本流程

Kafka Producer端默认使用的EventHandler实现是DefaultEventHandler，为消息发送的重点部分，我们首先看下handler()方法的基本流程：

    step1: Serialize Messages
      Retry Loop:
      	Update Metadata(timed)
          step2: Send Messages
          Fail:
          	Sleep
              Update Metadata(when failed)
      Fail:
      	throw FailedToSendMessageException
        
首先序列化messages，然后进入一个重试的循环，发送消息至Kafka集群，若失败接着重试。有两个地方会更新Meta信息，一个为定时更新，一个在发送失败后更新。最后重试结束后仍失败，则抛出FailedToSendMessageException。

一些可配置的参数如下：

    message.send.max.retries：消息发送最大重试次数，默认3
    topic.metadata.refresh.interval.ms：Meta信息更新周期，默认600000
    retry.backoff.ms：发送失败后，在更新Meta前Sleep的时间（这段时间Kafka集群可能在进行leader选举等操作，需sleep一段时间），默认100
    
## 消息序列化

serialize()方法的作用为将Seq[KeyedMessage[K,V]]转换为Seq[KeyedMessage[K,Message]]，其实主要就是用Message类将value进行封装为ByteBuffer。Message各个字节的含义如下：

    1. 4 byte CRC32 of the message
    2. 1 byte "magic" identifier to allow format changes, value is 2 currently
    3. 1 byte "attributes" identifier to allow annotations on the message independent of the version (e.g. compression enabled, type of codec used)
    4. 4 byte key length, containing length K
    5. K byte key
    6. 4 byte payload length, containing length V
    7. V byte payload

## 更新Meta信息

Producer会从Kafka集群同步Meta信息，主要包括TopicMetadata和PartitionMetadata。更新的时机有两个，一个为定时更新（由topic.metadata.refresh.interval.ms配置），另一个在消息发送失败后更新。

通过调用BrokerPartitionInfo的updateInfo()方法进行更新。首先通过ClientUtils.fetchTopicMetadata获取topicsMetadata，然后更新topicPartitionInfo: HashMap[String, TopicMetadata]变量，最后更新ProducerPool。下面简单说下ProducerPool。

ProducerPool含有一个syncProducers: new HashMap[Int, SyncProducer]，所有topic对应的leader partition所在的broker的id，都对应着一个SyncProducer，用于与Kafka集群建立连接，发送消息。当更新ProducerPool时，需将现有的所有SyncProducer关闭，然后再重新创建SyncProducer，建立连接。

## 消息分发（High Level）

消息分发是最重要也是较为复杂的部分，我分High Level和Low Level两个层次来讲。

我们首先看下消息发送的基本流程：

    step-1: message format convert, Seq[KeyedMessage[K,Message] => Map[Int,Map[TopicAndPartition,ByteBufferMessageSet]]
    step-2: for((brokerId, messageSetPerBroker)  buffer.putShort(requestId)
          case None =>
        }
        request.writeTo(buffer)
        buffer.rewind()
      }
          
      def writeCompletely(channel: GatheringByteChannel): Int = {
        var totalWritten = 0
        while(!complete)
        totalWritten += writeTo(channel)
        totalWritten
      }
      
      def writeTo(channel: GatheringByteChannel): Int = {
        var written = channel.write(Array(sizeBuffer, buffer))
        if(!buffer.hasRemaining)
          complete = true    
        written.asInstanceOf[Int]
      }
    }

BoundedByteBufferSend也很简单，传入需要发送的ByteBuffer。首先构造一个buffer，长度为request的sizeInBytes，若有requestId(short类型)，再加2个字节，调用request.writeTo(buffer)，用request的数据填充buffer；然后构造一个sizeBuffer，4个字节存入buffer的长度。调用writeCompletely时，循环调用writeTo，将sizeBuffer与buffer一并写入channel，直至buffer完全写完。

BoundedByteBufferReceive与BoundedByteBufferSend类似，在此不再赘述。

最后我们再看下ProducerRequest与ProducerResponse，这两个类是最终暴露给外部的（内部实际使用的是BoundedByteBufferSend与BoundedByteBufferReceive）。

ProducerRequest的构造：

    class ProducerRequest(versionId: Short = ProducerRequest.CurrentVersion,
                          correlationId: Int,
                          clientId: String,
                          requiredAcks: Short,
                          ackTimeoutMs: Int,
                          data: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) {
      def writeTo(buffer: ByteBuffer) // 将data写入buffer
    }
      
    object ProducerRequest {
      // 用于从buffer构造ProducerRequest
      def readFrom(buffer: ByteBuffer): ProducerRequest = {
        ProducerRequest(...)
      }
    }
    
ProducerResponse与ProducerRequest类似，在此不再赘述。

最后我们再来总结下syncProducer.send(request)流程：

    // 发送Request
    send(brokerId, Map[TopicAndPartition,ByteBufferMessageSet]) => SyncProducer.send(ProducerRequest) =>BlockingChannel.send(request) => new BoundedByteBufferSend(request).writeCompletely
    // 如果配置request.required.acks非0，则接受Reponse
    val response = blockingChannel.receive() => new BoundedByteBufferReceive().readCompletely(readChannel)
    ProducerResponse.readFrom(response.buffer)

一些配置如下：

    request.required.acks：控制发送请求何时结束返回
      0：立即返回
      1：Leader Partition写成功后返回
      -1：所有in-sync的Partition均写成功
    request.timeout.ms：配置socket的SoTimeout，默认10000
    send.buffer.bytes：配置socket的SendBufferSize，默认100*1024

## 关于异常

对于sync模式，send方法会直接调用handler.handle()，重试多次仍失败后会抛出FailedToSendMessageException异常。

对于async，send方法只是将message放入queue，最后由ProducerThread进行消费，在调用handler.handle()时加了try-catch，故即便发送失败ProducerThread线程也不会挂掉。

## 常用配置

* metadata.broker.list
* client.id
* serializer.class 
* producer.type
* compression.codec
* queue.enqueue.timeout.ms
* request.required.asks