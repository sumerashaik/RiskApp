name := "RiskApp"

scalaVersion := "2.10.4"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// protocol buffer support
// seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)

// additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-hive" % "1.5.0",
  "net.sf.opencsv" % "opencsv" % "2.3"
)

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)
