/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package spark.streaming.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.streaming.kafka010.{LocationStrategies, OffsetRange, KafkaUtils}
import collection.mutable._


/**


 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2

 sh spark-submit --class spark.streaming.demo2.Kafka_demo_limit \
 --jars /home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/spark-streaming-kafka-0-10_2.11-2.1.1.jar,/home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/kafka-clients-0.10.2.0.jar \
/home/yimr/sss/sparkDemo.jar 10.1.131.71:9092,10.1.131.72:9092,10.1.131.74:9092 kafkatopic_ods_2

  频繁在 java.util.map和scala map出现编译问题

  代码好像是不起作用

 */
object Kafka_demo_limit {

  private val master = "10.1.131.71"
  private val port = "7077"

  private val data_output = "file:///home/yimr/sss/data/out/tmp1"

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }


    val Array(brokers, topics) = args

    val sparkConf = new SparkConf()
      .set("spark.executor.memory","3g")
      .setMaster(s"spark://$master:$port")
      .setAppName("DirectKafkaWordCount")

    val sc = new SparkContext(sparkConf)

    var kafkaParams:java.util.HashMap[String, Object] = new java.util.HashMap()
    kafkaParams.put("bootstrap.servers" , brokers)
    kafkaParams.put("group.id" , "spark_stream_test1")
    kafkaParams.put("key.deserializer" , classOf[StringDeserializer])
    kafkaParams.put("value.deserializer" , classOf[StringDeserializer])
    kafkaParams.put("auto.offset.reset" , "latest")
    kafkaParams.put("enable.auto.commit" , "false")

    val offsetRanges = Array(
      // topic, partition, inclusive starting offset, exclusive ending offset
      OffsetRange(topics, 0, 0, 100),
      OffsetRange(topics, 1, 0, 100)
    )

    val locationStrategy = LocationStrategies.PreferConsistent

    val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams, offsetRanges, locationStrategy)

    rdd.repartition(1).saveAsTextFile(data_output)

  }
}
