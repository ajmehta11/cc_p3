import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FollowerDF {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path in parquet format.
    *
    * It must be done using the DataFrame/Dataset API.
    *
    * It is NOT valid to do it with the RDD API, and convert the result to a DataFrame, nor to read
    * the graph as an RDD and convert it to a DataFrame.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param spark the spark session.
    */
  def computeFollowerCountDF(inputPath: String, outputPath: String, spark: SparkSession): Unit = {
    // Read the input file as a DataFrame. Each line will be stored in a column named "value".
    val df = spark.read.text(inputPath)

    // Split each line on whitespace. The resulting array is stored in a new column "tokens".
    // Assuming each line is "follower followee", we extract the second token as the "followee".
    val splitDF = df.withColumn("tokens", split(col("value"), "\\s+"))
                    .withColumn("followee", col("tokens").getItem(1))

    // Group by the "followee" column and count the occurrences (i.e., the number of followers)
    val followersCountDF = splitDF.groupBy("followee").count()

    // Sort the results by follower count in descending order and limit to the top 100 users.
    val top100DF = followersCountDF.orderBy(desc("count")).limit(100)

    // Write the top 100 results as a Parquet file to the output path.
    top100DF.write.parquet(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val followerDFOutputPath = args(1)

    computeFollowerCountDF(inputGraph, followerDFOutputPath, spark)
  }
}
