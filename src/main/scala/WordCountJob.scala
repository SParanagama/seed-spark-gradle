import com.twitter.scalding.Args
import org.apache.spark.sql.SparkSession

/**
  * Created by sagara on 1/16/18.
  */
object WordCountJob {
  def main(args: Array[String]): Unit = {
    val params = Args(args)
    val ss = SparkSession.builder().enableHiveSupport().getOrCreate()
    val textFile = ss.sparkContext.textFile(params.required("input"))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile(params.required("output"))
  }
}
