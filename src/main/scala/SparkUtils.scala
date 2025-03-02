import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {

  /***
    * Returns a new SparkSession object.
    * Remember that some properties may not be affected when set programmatically through SparkConf.
    * Therefore, you should pass them as arguments to spark-submit when submitting the job.
    * @return a SparkSession.
    */
  def sparkSession(): SparkSession = {
    val conf = new SparkConf().setAppName("PageRank")
    SparkSession.builder.config(conf).getOrCreate()
  }

}
