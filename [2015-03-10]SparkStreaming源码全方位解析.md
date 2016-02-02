# SparkStreaming源码全方位解析

---

最近在做基于Kafka + Spark Streaming的实时计算，今天研究了下Spark Streaming源码，在此记录下。主要以WordCount为例，具体讲解Spark Streaming的实现细节。

---

## 从WordCount说起

一个最简单的基于Spark Streaming的WordCount，代码如下：

    object SocketWordCount extends App {
      val conf = new SparkConf().
          setMaster("local[*]").setAppName("WordCount")
      val ssc = new StreamingContext(conf, Seconds(10))
      val lines = ssc.socketTextStream("localhost", 9999)
      val wordCounts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      wordCounts.print
      ssc.start
      ssc.awaitTermination
    }

这个WordCount小程序很简单。首先创建一个SparkContext对象（与创建SparkContext不同，需要指定一个时间间隔）；然后通过ssc.socketTextStream创建InputDStream，然后对DStream进行各种transformation，调用print将结果输出；最后调用ssc.start启动程序即可。

更多Spark Streaming资料，详见官网教程 [Spark Streaming Programming Guide ](http://spark.apache.org/docs/latest/streaming-programming-guide.html)。

---

## 创建StreamingContext

> val ssc = new StreamingContext(conf, Seconds(10))

StreamingContext内部包含一个***SparkContext***，可以直接传入构造函数，或者通过传入的SparkConf新建；如果设置Checkpoint，可以通过Checkpoint.sparkConf新建。（注：为简化流程，后续解读均不涉及check point，write ahead log等细节）

除SparkContext，如下初始化组件需要注意：

***DSreamGraph***：主要含有一个inputStreams数组和一个outputStreams数组。

***JobScheduler***：调度SparkSteaming任务，主要包含一个ReceiverTracker（Receiver跟踪器），一个JobGenerator（JobGenerator生成器），以及一个JobScheduler Actor。

---

## 创建InputDSteam

> val lines = ssc.socketTextStream("localhost", 9999)

socketTextStream函数新建并返回了一个SocketInputDStream对象，其继承关系依次为：SocketInputDStream  val wordCounts = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
> wordCounts.print

Spark Core运算的基本单位是RDD，而Spark Streaming则是在***DStream***之上进行计算；RDD有一系列的transformation和action，DStream也有很多***[transformation](http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams)***和***[output operatioin](http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams)***。DStream是一个RDD的时间序列，其实最终计算还是会转移到RDD的计算上。

我们首先来看下***DStream***的构造（其实上节已经看其子类InputDSteam的实现）：

* 子类需实现如下三个函数
  * def slideDuration: Duration  // DStream生成一个RDD的时间间隔
  * def dependencies: List\[DStream\[\_\]\]  // 所依赖的父DStream（与RDD相似，也有依赖关系）
  * def compute (validTime: Time): Option[RDD[T]]  // 对于一个给定的时间生成一个RDD（应该还记得上一节ReceiverInputDStream返回的new BlockRDD\[T\](ssc.sc, blockIds)吧）
* 除此之外就是各种transformation和output operation的实现了。
* def getOrCompute(time: Time): Option\[\RDD\[T\]\]，我们需要注意下这个函数，DStream含有一个generatedRDDs: HashMap[\Time, RDD\[T\]\]，首先会看generatedRDDs中是否有time对应的RDDs，若有直接返回；否则，调用compute(time)进行计算；最后再将新计算的newRDD加入到generatedRDDs进行缓存。

下面我们分别选一个transformation和output operation实现来看下。

首先我们看下***flatMap***函数实现：

    def flatMap[U: ClassTag](flatMapFunc: T => Traversable[U]): DStream[U] = {
      new FlatMappedDStream(this, context.sparkContext.clean(flatMapFunc))
      }

很简单，返回了一个FlatMappedDStream。FlatMappedDStream实现也很简单，分别实现了slideDuration、dependencies、compute这三个函数。

    override def dependencies = List(parent)
    override def slideDuration: Duration = parent.slideDuration
    override def compute(validTime: Time): Option[RDD[U]] = {
     parent.getOrCompute(validTime).map(_.flatMap(flatMapFunc))
    }

通过Compute函数，可见其会调用getOrCompute，获取parent DStream在某个时间点的RDD，然后对RDD信息转换，生成新的RDD。

接下来，我们再来看下***print***函数的实现：

    def print() {
      def foreachFunc = (rdd: RDD[T], time: Time) => {
        val first11 = rdd.take(11)
        first11.take(10).foreach(println)
      }
      new ForEachDStream(this, context.sparkContext.clean(foreachFunc)).register()
    }

print()最后新建并返回了一个ForEachDStream，而所有output operation均是如此，我们再来看下ForEachDStream的实现：

    override def compute(validTime: Time): Option[RDD[Unit]] = None
    override def generateJob(time: Time): Option[Job] = {
      parent.getOrCompute(time) match {
        case Some(rdd) =>
          val jobFunc = () => {
            ssc.sparkContext.setCallSite(creationSite)
            foreachFunc(rdd, time)
          }
          Some(new Job(time, jobFunc))
        case None => None
      }
    }

其compute函数返回None，但是多了一个generateJob函数，生成new Job(time, jobFunc)对象，而Job之后会被调度。

---

## 启动StreamingContext

> ssc.start

很简单，启动***JobScheduler***，而JobScheduler接着启动了***ReceiverTracker***和***JobGenerator***。

ReceiverTracker主要负责原始数据的读入，而JobGenerator主要负责具体Job的触发与执行。下面我将分三个小节来分别讲解这两个核心组件，ReceiverTracker内容较多，分receiver启动和外部数据读取两个小节讲解。

---

## ReceiverTracker源码分析(一) receiver启动

ReceiverTracker启动后，会创建***ReceiverTrackerActor***，响应RegisterReceiver、AddBlock、ReportError、DeregisterReceiver事件。

接着启动***ReceiverLauncher***线程，这个线程将通过startReceivers函数，启动这个集群上的所有receivers。

我们看下***startReceivers***函数：

* 首先其从inputStreams获取所有的receivers
* 然后将其封装成为tempRDD
* 定义函数startReceiver
* 最后调用ssc.sparkContext.runJob(tempRDD, ssc.sparkContext.clean(startReceiver))函数，将所有的receiver通过封装成RDD，分发到集群的节点上，并启动startReceiver函数。（借住RDD分发任务，非常巧妙！）

下面重点庄转移到***startReceiver***函数，其新建了一个ReceiverSupervisorImpl，对receiver的一个包装类，然后启动supervisor，之后awaitTermination阻塞。

接着看***ReceiverSupervisorImpl***启动做了什么。调用两个函数onStart()和startReceiver()

* ***onStart()***由ReceiverSupervisorImpl实现，主要是启动blockGenerator。（ReceiverSupervisorImpl初始化时，会一并初始化blockGenerator，传入BlockGeneratorListener。BlockGenerator的具体含义在下一节会讲到）
* ***startReceiver()***由ReceiverSupervisor基类实现
  * 主要是调用***receiver.onStart()***，终于启动receiver了！！！
  * 然后调用ReceiverSupervisorImpl的onReceiverStart()，即为向ReceiverTrackerActor发送***RegisterReceiver***消息，将receiver加入元信息receiverInfo中。
  
***至此，Receiver的启动过程完毕！！！***

---

## ReceiverTracker源码分析(二) 外部数据读取

数据的读入由receiver触发，receiver启动后会读取外部数据源的消息，有两种方法将其存储：

* 调用***store(dataItem: T)***，存储单条消息
  * 最终调用receiver对应的ReceiverSupervisorImpl的pushSingle(dataItem)方法
  * pushSingle调用blockGenerator.addData(data)，将消息写入到
blockGenerator的currentBuffer中。（放入currentBuffer之前会有一个流控函数，配置参数spark.streaming.receiver.maxRate）
  * 上节讲到会启动blockGenerator。
      1. 会定时启动updateCurrentBuffer函数），将currentBuffer生成Block，放入blocksForPushing队列。（spark.streaming.blockInterval，默认200毫秒）
      2. 启动blockPushingThread线程，获取blocksForPushing队列中的blocks，并调用pushAndReportBlock方法。（调用比较曲折，先调用listener.onPushBlock，再调用ReceiverSupervisorImpl的pushArrayBuffer，最后再调用ReceiverSupervisorImpl的pushAndReportBlock）
* 调用store(dataBuffer: ArrayBuffer[T])，消息批量存储
  * 会调用receiver对应的ReceiverSupervisorImpl的pushArrayBuffer方法
  * 最后直接调用pushAndReportBlock(ArrayBufferBlock(arrayBuffer), metadataOption, blockIdOption)方法
  
可见使用store(dataItem: T)***无需自己生成Block***，且有自动流控措施，但是当receiver挂掉的时候currentBuffer中的messages和blocksForPushing中的blocks均有可能会丢失。所以Unreliable Receivers可以使用，而对于Reliable Receivers，必须使用store(dataBuffer: ArrayBuffer[T])。详见官网 [Custom Receiver Guide](http://spark.apache.org/docs/latest/streaming-custom-receivers.html)

最后再来说一说***pushAndReportBlock***方法：

* 首先获取一个blockId
* 然后调用 val blockStoreResult = receivedBlockHandler.storeBlock(blockId, receivedBlock)，接着调用BlockManagerBasedBlockHandler的storeBlock，最后 blockManager.putIterator，***将Block信息存入blockManage！！！***
* 最后向ReceiverTrackerActor发送AddBlock消息，将ReceivedBlockInfo(streamId, numRecords, blockStoreResult)，ReceiverTrackerActor接着调用receivedBlockTracker.addBlock(receivedBlockInfo)，最后加入getReceivedBlockQueue(receivedBlockInfo.streamId) += receivedBlockInfo。***将Block信息存入ReceiverTrackerActor的streamIdToUnallocatedBlockQueues，供计算使用！！！***

***至此，外部数据读取过程完毕！！！***

---

## JobGenerator源码分析

JobGenerator启动后，创一个***JobGeneratorActor***，响应GenerateJobs(time)、ClearMetadata(time)、DoCheckpoint(time)、ClearCheckpointData(time)等事件。

然后调用startFirstTime函数，依次启动graph（就是inputStreams和outputStreams的一些初始化，不讲了），并且根据配置的batchDuration定时向JobGeneratorActor发送GenerateJobs消息。

OK，来看看GenerateJobs做了什么：

* receiverTracker.allocateBlocksToBatch(time)  // 从streamIdToUnallocatedBlockQueues中获取这个batchDuration的所有streamId对应Blocks，加入到timeToAllocatedBlocks(batchTime) = allocatedBlocks
* val jobs = graph.generateJobs(time)  // 生成这个batchDuration内所有的jobs
  * 遍历所有的outputStreams，分别调用其generateJob(time)方法，生成job
* val receivedBlockInfos = jobScheduler.receiverTracker.getBlocksOfBatch(time)	// 获取timeToAllocatedBlocks(batchTime)对应的所有blocks
* jobScheduler.submitJobSet(JobSet(time, jobs, receivedBlockInfos))  // 提交JobSet
  * 遍历JobSet中的每个job，讲new JobHandler(job)加入线程池jobExecutor执行（线程池大小：spark.streaming.concurrentJobs，默认为1）
  * JobHandler开始执行时，首先向JobGeneratorActor发送JobStarted消息，然后调用job的run()方法，进而调用func()函数，及最后的foreachFunc()。foreachFunc最终将作用于这个batchDuration的outputStream对应的RDD上，***进而产生Spark的任务！！！***
  * 执行完毕后，向JobGeneratorActor发送JobCompleted消息。
  
***至此，Job调度过程完毕！！！***

---

## 总结：Spark Streaming与Spark Core的联系

总体来说，Spark Streaming的实现以Spark Core为基础，通过ReceiverTracker来读取外部数据，通过JobGenerator定期生成计算任务，整体结构实现清晰明确。

Spark Streaming用到Spark Core的地方在总结下：

1. Receiver分发至各个节点并执行，使用了Spark Core提交RDD任务的过程，很巧妙；
2. 外部数据源读入的数据存入BlockManager；
3. 对于InputDStream，每隔batchDuration切分的RDD，DStream间的transformation，即为RDD的transformation；
4. 提交的任务最终转化为一个Spark Core的RDD计算任务。