import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object PageRank {

  // Do not modify
  val PageRankIterations = 10

  /**
    * Input graph is a plain text file of the following format:
    *
    *   follower  followee
    *   follower  followee
    *   follower  followee
    *   ...
    *
    * where the follower and followee are separated by `\t`.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `outputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * where node and rank are separated by `\t`.
    *
    * @param inputGraphPath path of the input graph.
    * @param outputPath path of the output of page rank.
    * @param iterations number of iterations to run on the PageRank.
    * @param spark the SparkSession.
    */
  def calculatePageRank(
      inputGraphPath: String,
      graphTopicsPath: String,
      pageRankOutputPath: String,
      recsOutputPath: String,
      iterations: Int,
      spark: SparkSession): Unit = {
    val sc = spark.sparkContext
    import spark.implicits._

    // Constants for PageRank
    val dampingFactor = 0.85
    
    // Step 1: Load the graph data
    // Parse the input graph into a DataFrame with follower and followee columns
    val graphDF = spark.read.text(inputGraphPath)
      .map(row => {
        val parts = row.getString(0).split("\t")
        // Parse user IDs as Long for better performance
        (parts(0).toLong, parts(1).toLong)
      }).toDF("follower", "followee")
    
    // Step 2: Find all unique users (nodes) in the graph
    val allUsersDF = graphDF.select($"follower".alias("user"))
      .union(graphDF.select($"followee".alias("user")))
      .distinct()
    
    // Count total number of users in the graph
    val numUsers = allUsersDF.count()
    
    // Step 3: Initialize PageRank scores to 1/N for all users
    var ranksDF = allUsersDF.withColumn("rank", lit(1.0 / numUsers))
    
    // Step 4: Compute outgoing links for each user
    val outgoingLinksDF = graphDF.groupBy("follower")
      .agg(collect_list("followee").alias("followees"), 
           count("followee").alias("numFollowing"))
    
    // Step 5: Run PageRank iterations
    for (i <- 1 to iterations) {
      // Calculate contributions from each user to their followees
      val contributionsDF = ranksDF.join(outgoingLinksDF, 
                                        ranksDF("user") === outgoingLinksDF("follower"), 
                                        "inner")
        .withColumn("contribution", $"rank" / $"numFollowing")
        .select($"followees", $"contribution")
        .withColumn("followee", explode($"followees"))
        .select($"followee", $"contribution")
      
      // Aggregate contributions received by each followee
      val summedContributionsDF = contributionsDF
        .groupBy("followee")
        .agg(sum("contribution").alias("received_contrib"))
      
      // Identify dangling nodes (users who don't follow anyone)
      val danglingNodesDF = ranksDF.join(
        outgoingLinksDF.select("follower"), 
        ranksDF("user") === outgoingLinksDF("follower"), 
        "left_anti"
      )
      
      // Calculate total dangling node mass to redistribute
      val danglingMass = danglingNodesDF.agg(sum("rank")).first().getDouble(0)
      val danglingRedistribution = danglingMass / numUsers
      
      // Compute new ranks with PageRank formula:
      // rank = (1-d)/N + d * (received_contributions + danglingMass/N)
      val newRanksDF = allUsersDF
        .join(summedContributionsDF, 
              allUsersDF("user") === summedContributionsDF("followee"), 
              "left_outer")
        .withColumn("rank", 
                    lit((1.0 - dampingFactor) / numUsers) + 
                    lit(dampingFactor) * (coalesce($"received_contrib", lit(0.0)) + lit(danglingRedistribution))
        )
        .select($"user", $"rank")
      
      // Update ranks for next iteration
      ranksDF = newRanksDF
    }
    
    // Step 6: Save the final PageRank results in the required format
    ranksDF.select($"user".cast(StringType), $"rank".cast(StringType))
      .map(row => row.getString(0) + "\t" + row.getString(1))
      .write.text(pageRankOutputPath)
    
    // Note: The implementation for user recommendations (Task 2.2) 
    // would go here, but is not included in this solution
    // This is just the PageRank implementation

    // Placeholder for recommendations output - implement Task 2.2 here
    // In a real implementation, you would generate recommendations and write them to recsOutputPath
  }

  /**
    * @param args it should be called with two arguments, the input path, and the output path.
    */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()

    val inputGraph = args(0)
    val graphTopics = args(1)
    val pageRankOutputPath = args(2)
    val recsOutputPath = args(3)

    calculatePageRank(inputGraph, graphTopics, pageRankOutputPath, recsOutputPath, PageRankIterations, spark)
  }
}