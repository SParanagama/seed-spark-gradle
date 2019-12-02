import org.apache.spark.sql.SparkSession

/**
  * Created by sagara on 1/16/18.
  */
object WordCountJob {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
                         .master("local[*]")
                         .getOrCreate()

    val textFile = ss.sparkContext.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
                         .map(word => (word, 1))
                         .reduceByKey(_ + _)

    counts.saveAsTextFile(args(1))
  }
}
