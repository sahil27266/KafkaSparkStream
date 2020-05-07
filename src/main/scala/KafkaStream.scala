import org.apache.log4j._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaStream extends App {

  val conf=new SparkConf()
    .setAppName("kafkastream")
    .setMaster("local[12]")  //you can give any number of cores as per your desire but >1
  val ssc=new StreamingContext(conf,Seconds.apply(1))
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val topics = Array("topicX")
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  val keyValuePairs=stream.map(record => (record.key, record.value))
  val values=keyValuePairs.map(x=>x._2)
  val words=values.flatMap(_.split(" "))
  val wordsCount=words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
  wordsCount.print(100000000)
   //These are the optional settings to avoid printing all logs in console

  val rootLogger=Logger.getRootLogger
  val level=rootLogger.setLevel(Level.ERROR)


  ssc.start()
  ssc.awaitTermination()


}