import org.apache.spark.SparkContext

object FollowerRDD {

  /**
    * This function should first read the graph located at the input path, it should compute the
    * follower count, and save the top 100 users to the output path with userID and
    * count **tab separated**.
    *
    * It must be done using the RDD API.
    *
    * @param inputPath the path to the graph.
    * @param outputPath the output path.
    * @param sc the SparkContext.
    */
  def computeFollowerCountRDD(inputPath: String, outputPath: String, sc: SparkContext): Unit = {
    // Load the graph from the input path.
    val lines = sc.textFile(inputPath)
    
    // Each line is assumed to be "follower followee" (whitespace separated)
    // We map each line to (followee, 1)
    val followeePairs = lines.map(line => {
      val tokens = line.split("\\s+")
      // tokens(1) is the followee
      (tokens(1), 1)
    })

    // Reduce by key to count followers for each followee.
    val followerCounts = followeePairs.reduceByKey(_ + _)
    
    // Sort the results by the count in descending order.
    val sortedCounts = followerCounts.sortBy({ case (_, count) => count }, ascending = false)
    
    // Extract the top 100 popular users.
    val top100 = sortedCounts.take(100)
    
    // Format each result to "User_id<TAB>num_followers"
    val formattedResults = sc.parallelize(top100.map { case (user, count) => s"$user\t$count" })
    
    // Save the results to the output path.
    formattedResults.saveAsTextFile(outputPath)
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputGraph = args(0)
    val followerRDDOutputPath = args(1)

    computeFollowerCountRDD(inputGraph, followerRDDOutputPath, sc)
  }
}
