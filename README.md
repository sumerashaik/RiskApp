sbt clean package

spark-submit --class sumera.RiskDriver --master yarn target/scala-2.10/riskapp_2.10-0.1-SNAPSHOT.jar
