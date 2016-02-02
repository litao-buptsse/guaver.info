# Spark-JobServer "pemgen space"异常分析

---

在搜狗，我们常使用[spark-jobserver](https://github.com/spark-jobserver/spark-jobserver)来支持long time running service的spark应用，主要为了hold住资源，减少频繁与YARN进行资源申请的调度开销。但近期使用过程中遇到"pemgen space"异常，下面来分析下原因并给出解决方案。

---

## 确定为内存泄露

首先，我试图增大Pem至更大的内存，发现然并卵，能撑的时间更长些，但最后仍报"pemgen space"。

    -XX:MaxPermSize=4G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled

之后通过jvisualvm工具发现load的classes数量在持续飙高，毫无下降趋势，故基本确定是内存泄露。

---

## load了哪些classes？

在得知是classes内存泄露后，很直观的想法就是找到哪些classes在不停load。java有一个命令行参数可以打印出每次classes load的信息，发现确实再不停的进行class load，有相当一部分是spark的类（更准确说是spark-sql的类），并没有spark-jobserver的类。

    -verbose:class

---

## 什么操作触发classes load？

通过jvisualvm的classes数量实时曲线，发现classes的数量并非直线增长，而是呈现阶段性增长。进而发现，classes增长与任务提交一致，每当提交任务至spark-jobserver执行，classes数量便增长。

---

## spark-jobserver任务提交过程

在得知是任务提交导致classes load后，便去看spark-jobserver在任务提交这块的源码。具体源码细节不再赘述，会点[akka](http://akka.io/)和[spray](http://spray.io/)看起来很容易，下面只是简单说下大概流程。

提交任务之前，应首先创建一个context，用于hold住资源，创建一个JobManagerActor（一个context对应一个JobManagerActor，管理这个context下所有的job），并将SparkContext创建出来。

接着任务提交，即向对应的JobManagerActor发送一个JobManagerActor.StartJob消息，根据用户配置的main class（实现SparkJob接口）实例化对象，然后调用重写的runJob方法，传入之前事先穿件的sc，进行最终的任务执行。

---

## 怀疑spark-jobserver的classLoader

了解任务的提交过程后，首先怀疑是不是spark-jobserver的代码写尿了（毕竟不是apache项目）。比如，会不会每个任务都单独一个线程执行，然后每个线程每次都new一个classLoader？

接着继续看JobManagerActor的代码，发现确实有一个jarLoader成员变量引起怀疑，不过后来这个疑惑取消了。

首先JobManagerActor只有一个对象，而jarLoader是其成员变量也只实例化一次。对于Actor而言，其有一个mailbox，可以并发的向其发送消息，但actor并上对于mailbox是串行执行的，故是线程安全的。我们看到对于每个任务在执行时会有以下语句，jarLoader还是那个成员变量，也没有什么问题。

    Thread.currentThread.setContextClassLoader(jarLoader)

而且spark-jobserver对于每个JobManagerActor有一个jarLoader成员变量是合理的。因为不同的app的jar不同，但是同一个app不同的job间jar是相同的。如果只有一个classLoader，对于多个app会有class冲突的风险；而如果每次运行一个job就load一次classes效率又过低。故对于一个app（或者说context，目前spark的multi context实现还有问题）配置一个jobLoader，被所有job共享是合适的。

---

## 发现多为spark-sql相关的类

spark-jobserver实现没有问题，那会不会是spark的问题？再去看了下每次新load的classes，所谓spark-sql的类，而spark-core的类只被load了一次，奇怪！而这也正是一个很好的切入点！

再去看了下app的代码，发现确实用到了HiveContext，感觉应该是这块的坑。

---

## spark-jobserver新feature：支持SQLContext、HiveContext、StreamingContext

还好前几天升级spark-jobserver时，读了下新的release note，发现spark-jobserver支持SQLContext、HiveContext、StreamingContext。当时的感觉就是没啥乱用，spark原生的sql-thrift-server和spark-streaming都有hold住context的能力，还用这个spark-jobserver有个屌用。然而，我发现也正是这块出了问题。

接着之前的分析，spark-core的类只load了一次，spark-sql的类却load了多次，太奇怪了，为什么？

比如看下SparkContext和HiveContext两个对象的实例化的时机，SparkContext只有spark-jobserver在初始时实例化一次，而HiveContext在每次提交任务时都会通过new HiveContext(sc)新建多次。

    object WAPRealtimeSparkClient extends SparkJob {
      override def runJob(sc: SparkContext, config: Config): Any = {
        val time = config.getString("time")
        new WAPRealtimeRunner(sc, time).call
      }
      
      override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
        Try(config.getString("time"))
          .map(_ => SparkJobValid)
          .getOrElse(SparkJobInvalid("No time config param"))
      }
    }
      
    class WAPRealtimeRunner(@transient sc: SparkContext, time: String)
      extends CallableWithKillable[Int] with Serializable {
      override def call(): Int = {
        val sqlContext = new HiveContext(sc)
        ......
      }
    }

---

## 尝试直接在spark-jobserver实例化HiveContext

看了上面的代码感觉极有可能是HiveContext的问题，最简单的办法，为何不尝试直接在spark-jobserver实例化一个HiveContext，而非SparkContext（正好用用spark-jobserver的这个新feature）。

查了下官网只需要修改两个地方：

第一，创建context时候增加context-factory=spark.jobserver.context.HiveContextFactory配置，表明创建HiveContext而非默认的SparkContext

    context-factory = spark.jobserver.context.HiveContextFactory

第二，app的代码需要实现SparkHiveJob接口，这时runJob和validate传入的参数直接就是hiveContext而非sc了。

    object WAPRealtimeSparkClient extends SparkHiveJob {
      override def runJob(hiveContext: HiveContext, config: Config): Any = {
        val time = config.getString("time")
        new WAPRealtimeRunner(hiveContext, time).call
      }
      
      override def validate(hiveContext: HiveContext, config: Config): SparkJobValidation = {
        Try(config.getString("time"))
          .map(_ => SparkJobValid)
          .getOrElse(SparkJobInvalid("No time config param"))
      }
    }

当然，为了编译通过，还需在sbt配置文件中，增加job-server-extras的依赖

```
libraryDependencies += "spark.jobserver" %% "job-server-extras" % "0.5.2" % "provided"
```

---

## 编译打包，重新创建hiveContext，重新提交任务，classes不在增加！

重新编译app代码，重新创建hiveContext，重新提交任务后，classes不在增加了了，bug修复了！原因也好理解，第一次实例化时就把HiveContext实例化了，相关的classes已经load，之后在运行时classes已经存在，便无需继续load了。

---

## 试图解释之前为什么不行，看下HiveContext的实现

简单看下了HiveContext的源码，确实每次实例化时，会同时实例化一个自己的IsolatedClientLoader，故每次都会而非spark-jobserver中的jarLoader，故确实每次实例化HiveContext时，都会load classes。

---

## 总结

本次"pemgen space"异常成功解决，总结一下分析解决问题的经验：

1. 收集数据，合理分析，找突破口，大胆推断
2. 熟悉源码，勤于思考
3. 紧跟开源进展，对新feature有所了解

---

## 资料

[spark-jobserver](https://github.com/spark-jobserver/spark-jobserver/)

[issue: pemgen space memory leak](https://github.com/spark-jobserver/spark-jobserver/issues/246)