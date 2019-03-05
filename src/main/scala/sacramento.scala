import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.split


  object sacramento {
    val sparkConf = new SparkConf().setAppName("Sacramento App").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // DataFrame

    // df - Sacramento crimes in Jan 2006
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("C:\\Users\\Student\\IdeaProjects\\spark_scala_day2\\src\\main\\resources\\SacramentocrimeJanuary2006.csv")

    // df2 - crime codes
    val df2 = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("C:\\Users\\Student\\IdeaProjects\\spark_scala_day2\\src\\main\\resources\\ucr_ncic_codes.tsv")

    // RDD
    // rdd - Sacramento crimes in Jan 2006
    val rdd = df.rdd

    // rdd2 - crime codes
    val rdd2 = df2.rdd

    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block    // call-by-name
      val t1 = System.nanoTime()
      println("Elapsed time: " + (t1 - t0) + "ns")
      result
    }

}
