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

import org.apache.kafka.clients.producer.ProducerRecord
import spark.streaming.kafka.kafka_writer._
import org.apache.kafka.common.serialization.{StringSerializer, StringDeserializer}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD



/**


 * Example:
 *    $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
 *    topic1,topic2

 nohup sh spark-submit --class spark.streaming.demo2.Kafka_demo_writer \
 --jars /home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/spark-streaming-kafka-0-10_2.11-2.1.1.jar,/home/yimr/sss/spark-2.1.0-bin-hadoop2.7/jars/kafka-clients-0.10.2.0.jar \
/home/yimr/sss/sparkDemo.jar 10.1.131.71:9092,10.1.131.72:9092,10.1.131.74:9092 kafkatopic_flux_3 > /home/yimr/tmp18.txt &

 13001489999|31192120|6941550mpy4|12744|29614|8604710057196401|101|311921365758030|2480|46876|2|200|5|460011480058683|http://10.17.170.2/fT2L5b+fpLND|0||018|0180|10.91.128.19|10.0.0.172|220.206.133.68|220.206.133.131|1026|80|Zte-tu235_TD/1.0 ThreadX/4.0b Larena/2.40 Release/4.15.2010 Browser/NetFront3.5 Profile/MIDP-2.0 Configuration/CLDC-1.1|application/vnd.wap.mms-message

 生产者可以决定分区，一般使用默认分区，根据key的hashcode和分区数进行分区

 参考网址：
  https://github.com/BenFradet/spark-kafka-writer

 使用的topic
  kafkatopic_flux_1
  kafkatopic_flux_2
  kafkatopic_flux_3
  kafkatopic_ods_2

 */
object Kafka_demo_writer {

  private val master = "10.1.131.71"
  private val port = "7077"

  private val data_input = "hdfs://10.1.131.72:8020/user/yimr/Modeler/flux-hb/once/131*.txt.gz"
  //private val data_input = "hdfs://10.1.131.72:8020/user/yimr/Modeler/odsdata/once/hb1-18/18_tg_cdrmm_201307/18_tg_cdrmm_201307_01700001.txt.gz"

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
      .set("spark.executor.memory","40g")
      .setMaster(s"spark://$master:$port")
      .setAppName("DirectKafkaWordCount")

    val sc = new SparkContext(sparkConf)

    val producerConfig = Map(
      "bootstrap.servers" -> brokers,
      "key.serializer" -> classOf[StringSerializer].getName,
      "value.serializer" -> classOf[StringSerializer].getName
    )

    val rdd: RDD[String] = sc.textFile(data_input)

    rdd.writeToKafka(
      producerConfig,
      s => new ProducerRecord[String, String](topics, s.split("\\|")(0),s)
    )

  }
}
