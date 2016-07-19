package sumera

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object RiskDriver extends App {

  val sc = new SparkContext(new SparkConf().setAppName("Risk Application"))
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  import org.apache.spark.sql.functions._
  // Create a simple DataFrame with a single column called "id"
  // containing number 5 to 10.
  val df = sqlContext.range(5, 11)
  // Create an instance of UDAF GeometricMean.
  val vm = new VaRMean
  // mean of values of column "id".
  df.groupBy().agg(vm(col("id")).as("VaRMean")).show()
  // Register the UDAF and call it "gm".
  sqlContext.udf.register("vm", vm)
  // Invoke the UDAF by its assigned name.
  df.groupBy().agg(expr("vm(id) as VaRMean")).show()

}
