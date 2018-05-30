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
package spark.streaming.kafka.outputFormat

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.io.Text
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent


/**


 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2

 sh spark-submit --class spark.streaming.chinaunicom.Kafka_demo_outputFormat \
 --jars /home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/spark-streaming-kafka-0-10_2.11-2.1.1.jar,/home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/kafka-clients-0.10.2.0.jar \
/home/yimr/sss/sparkDemo.jar 10.1.131.71:9092,10.1.131.72:9092,10.1.131.74:9092 kafkatopic_flux_3

 问题：
   每条数据，按照数据内容设置输出的文件。数据接收量，会非常的低。几乎无法使用

 */
object Kafka_demo_outputFormat {

  private val master = "10.1.131.71"
  private val port = "7077"

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
      .set("spark.executor.memory","30g")
      .set("spark.streaming.kafka.maxRatePerPartition", "1000")
      .setMaster(s"spark://$master:$port")
      .setAppName("DirectKafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "group.id" -> "new_3_3",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> "false"
    )


    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print

    println("directStream")

    stream.map(iter =>(iter.value(), iter.value().split("\\|")(7).substring(0,2) )).saveAsHadoopFiles("","",classOf[Text],classOf[Text],classOf[MyMultipleTextOutputFormat[Text,Text]])

    ssc.start()
    ssc.awaitTermination()
  }

  def timeFormat_2(time:String):String={
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date: String = sdf.format(new Date((time.toLong)))
    date
  }


}
