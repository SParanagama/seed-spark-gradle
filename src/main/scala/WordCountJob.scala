import org.apache.spark.sql.SparkSession

/**
  * Created by sagara on 1/16/18.
  */
object WordCountJob {
  def main(args: Array[String]): Unit = {

    val inputDataLocation = args(0)
    val outputResultLocation = args(1)

    val ss = SparkSession.builder()
                         .master("local[*]")
                         .getOrCreate()

    val textFile = ss.sparkContext.textFile(inputDataLocation)
    val counts = textFile.flatMap(line => line.split(" "))
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)

    counts.saveAsTextFile(outputResultLocation)
  }
}
