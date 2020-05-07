name := "kafkastreamingspark"

version := "0.1"

scalaVersion := "2.11.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.5"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "2.4.5"
