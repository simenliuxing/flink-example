package com.dtstack.flink.scala.datastream

import com.dtstack.flink.scala.datastream.function.MyBroadcastProcessFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies.FailureRateRestartStrategyConfiguration
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.connector.jdbc.JdbcInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import java.util.concurrent.TimeUnit

/**
 * @author xiuyuan
 */
object FlinkStreamExample {
  def main(args: Array[String]): Unit = {


    //1、明确查询结果字段的类型
    val fieldType: Array[TypeInformation[_]] = Array[TypeInformation[_]](BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    //2、将查询结果封装成一个ROW对象
    // val rowTypeInfo = new RowTypeInfo(fieldType)

    val rowTypeInfo = new RowTypeInfo(fieldType, Array("book_id", "book_name"))
    //3、连接数据库，并进行查询操作
    //JDBCInputFormat 需要引入org.apache.flink.api.java.io.jdbc.JDBCInputFormat
    val jdbcInputFormat: JdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
      .setDrivername("com.mysql.cj.jdbc.Driver")
      .setDBUrl("jdbc:mysql://localhost:3306/test?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC")
      .setUsername("root")
      .setPassword("root")
      .setQuery("select book_id,book_name from test_book")
      .setRowTypeInfo(rowTypeInfo) //设置返回的结果数据类型
      .finish //清空连接


    //获取一个执行环境（execution environment）；
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    environment.enableCheckpointing(300000)
    environment.getCheckpointConfig.setCheckpointTimeout(300000)
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(60000)

    //开启重启策略
    //6分钟内可以重启3次 每次重启都间隔10秒
    environment.setRestartStrategy(
      new FailureRateRestartStrategyConfiguration(3,
        Time.of(6, TimeUnit.MINUTES),
        Time.of(10, TimeUnit.SECONDS))
    )

    //添加kafka的配置文件
    val props: Properties = new Properties()
    props.put("bootstrap.servers", "172.16.100.109:9092")
    props.put("group.id", "dtstack")

    val consumer = new FlinkKafkaConsumer("testOut", new SimpleStringSchema(), props)
    //设置kafka的消费机制
    consumer.setStartFromEarliest()
    //数据流表，即SQL中的源表
    val ds: DataStream[(Int, Int, Int)] = environment.addSource(consumer).map(s => {
      val jn: JsonNode = new ObjectMapper().readTree(s)
      (jn.get("user_id").asInt(), jn.get("book_id").asInt(), jn.get("browse").asInt())
    })


    //维表，数据库中用于补充数据的数据表
    val sideTab: DataStream[(Int, String)] = environment.createInput(jdbcInputFormat).map(s => {
      val str: Array[String] = s.toString.split(",")
      (str(0).toInt, str(1))
    })
    //mapStateDescriptor 用来描述状态中存储值的类型 key: bookId, value:bookName
    val broadcastStateDescriptors: MapStateDescriptor[Int, (Int, String)] = new MapStateDescriptor[Int, (Int, String)]("msd", createTypeInformation[Int], createTypeInformation[(Int, String)])

    val bs: BroadcastStream[(Int, String)] = sideTab.broadcast(broadcastStateDescriptors)

    //通过数据流去关联状态中的值（图书维表），并通过process方法处理函数
    val bcs: BroadcastConnectedStream[(Int, Int, Int), (Int, String)] = ds.connect(bs)
    //对广播流的数据补全所需信息
    val value: DataStream[(Int, String, Int)] = bcs.process(new MyBroadcastProcessFunction(broadcastStateDescriptors))

    //对数据进行聚合
    //窗口计算聚合，reduce（输入输出结果必须相同） 和 aggregate（输入输出的格式可以不一样）
    val result: DataStream[(Int, String, Int)] = value.keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)))
      .reduce((v1, v2) => (v1._1, v1._2, v1._3 + v2._3))
    //.aggregate(new MyAggregateFunction)


    result.print()

    //触发程序执行，等待作业完成，返回一个 JobExecutionResult，其中包含执行时间和累加器结果
    environment.execute("FlinkStreamExample")
  }
}
