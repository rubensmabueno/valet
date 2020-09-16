name := "valet"

version := "1.0"

scalaVersion := "2.12.12"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

mainClass in (Compile, run) := Some("com.rubensminoru.Consumer")

lazy val akkaVersion = "2.6.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.apache.kafka" %% "kafka" % "2.6.0",
  "io.confluent" % "kafka-avro-serializer" % "5.5.1",
  "org.apache.parquet" % "parquet-avro" % "1.11.1",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test,
  "org.scalatest" %% "scalatest" % "3.1.0" % Test
)
