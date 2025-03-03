import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.Window

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
    
    // Optimization: Calculate a good partition number based on data size
    val numPartitions = 200  // Adjust based on cluster size
    
    // Step 2: Find all unique users (nodes) in the graph
    val allUsersDF = graphDF.select($"follower".alias("user"))
      .union(graphDF.select($"followee".alias("user")))
      .distinct()
      .repartition(numPartitions) // Repartition allUsersDF for better distribution
    
    // Count total number of users in the graph
    val numUsers = allUsersDF.count()
    
    // Step 3: Initialize PageRank scores to 1/N for all users
    var ranksDF = allUsersDF.withColumn("rank", lit(1.0 / numUsers))
    
    // Step 4: Compute outgoing links for each user
    // Optimization: Repartition and cache the outgoing links
    val outgoingLinksDF = graphDF.groupBy("follower")
      .agg(collect_list("followee").alias("followees"), 
           count("followee").alias("numFollowing"))
      .repartition(numPartitions, $"follower") // Repartition by join key
      .cache()
    
    // ----------------------------------------------------------------------
    // NEW CODE: Load topic frequency data for recommendation system
    // ----------------------------------------------------------------------
    val topicsDF = spark.read.text(graphTopicsPath)
      .map(row => {
        val parts = row.getString(0).split("\t")
        // Parse user ID and topic frequencies
        (parts(0).toLong, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble)
      }).toDF("user", "games_freq", "movies_freq", "music_freq")
      .repartition(numPartitions)
      .cache()
    
    // Calculate which topics each user is interested in (frequency >= 3.0)
    val userInterestsDF = topicsDF
      .withColumn("games_interest", when($"games_freq" >= 3.0, true).otherwise(false))
      .withColumn("movies_interest", when($"movies_freq" >= 3.0, true).otherwise(false))
      .withColumn("music_interest", when($"music_freq" >= 3.0, true).otherwise(false))
      .cache()
    
    // Initialize recommendation tracking data
    // For each user, track maximum frequency for each topic and source user
    var recTrackerDF = userInterestsDF.select(
      $"user",
      // For each topic, initialize with user's own frequency if interested, otherwise 0
      when($"games_interest", $"games_freq").otherwise(0.0).as("games_max_freq"),
      when($"movies_interest", $"movies_freq").otherwise(0.0).as("movies_max_freq"),
      when($"music_interest", $"music_freq").otherwise(0.0).as("music_max_freq"),
      // Track source user ID for each topic's max frequency (initially self)
      when($"games_interest", $"user").otherwise(-1L).as("games_max_user"),
      when($"movies_interest", $"user").otherwise(-1L).as("movies_max_user"),
      when($"music_interest", $"user").otherwise(-1L).as("music_max_user"),
      // Keep original interest flags and frequencies for filtering
      $"games_interest", $"movies_interest", $"music_interest",
      $"games_freq", $"movies_freq", $"music_freq"
    )
    
    // ----------------------------------------------------------------------
    // Step 5: Run PageRank iterations with recommendation system
    // ----------------------------------------------------------------------
    for (i <- 1 to iterations) {
      // Optimization: Ensure ranks DF is properly partitioned for join
      if (i > 1) {
        ranksDF = ranksDF.repartition(numPartitions, $"user")
      }
      
      // Calculate contributions from each user to their followees
      val contributionsDF = ranksDF.join(
        outgoingLinksDF, 
        ranksDF("user") === outgoingLinksDF("follower"), 
        "inner"
      )
      .withColumn("contribution", $"rank" / $"numFollowing")
      .select($"followees", $"contribution", $"follower")
      
      // ----------------------------------------------------------------------
      // NEW CODE: Calculate recommendation contributions
      // ----------------------------------------------------------------------
      // Join with recommendation tracker to include topic data in contributions
      val recContributionsDF = contributionsDF.join(
        recTrackerDF,
        contributionsDF("follower") === recTrackerDF("user"),
        "inner"
      ).select(
        $"follower", $"followees", $"contribution",
        // Only pass topic data for topics the user is interested in (filtering rule)
        when($"games_interest", $"games_max_freq").otherwise(0.0).as("games_max_freq"),
        when($"movies_interest", $"movies_max_freq").otherwise(0.0).as("movies_max_freq"),
        when($"music_interest", $"music_max_freq").otherwise(0.0).as("music_max_freq"),
        when($"games_interest", $"games_max_user").otherwise(-1L).as("games_max_user"),
        when($"movies_interest", $"movies_max_user").otherwise(-1L).as("movies_max_user"),
        when($"music_interest", $"music_max_user").otherwise(-1L).as("music_max_user")
      )
      
      // Explode followees to create individual contribution records
      val explodedContributionsDF = recContributionsDF
        .withColumn("followee", explode($"followees"))
        .select(
          $"follower", $"followee", $"contribution",
          $"games_max_freq", $"movies_max_freq", $"music_max_freq",
          $"games_max_user", $"movies_max_user", $"music_max_user"
        )
      
      // Optimization: Repartition by followee before aggregation to reduce shuffle
      val repartitionedContributionsDF = explodedContributionsDF
        .repartition(numPartitions, $"followee")
      
      // Aggregate contributions received by each followee for PageRank
      val summedContributionsDF = repartitionedContributionsDF
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
      
      // Compute new ranks with PageRank formula
      // Optimization: Join on well-partitioned data
      val newRanksDF = allUsersDF
        .join(
          summedContributionsDF,
          allUsersDF("user") === summedContributionsDF("followee"),
          "left_outer"
        )
        .withColumn("rank", 
                    lit((1.0 - dampingFactor) / numUsers) + 
                    lit(dampingFactor) * (coalesce($"received_contrib", lit(0.0)) + lit(danglingRedistribution))
        )
        .select($"user", $"rank")
      
      // ----------------------------------------------------------------------
      // NEW CODE: Compute updated recommendations
      // ----------------------------------------------------------------------
      // Aggregate recommendation data by followee
      val recAggrDF = repartitionedContributionsDF.groupBy("followee").agg(
        // For each topic, get maximum frequency among all contributors
        max("games_max_freq").alias("games_freq_agg"),
        max("movies_max_freq").alias("movies_freq_agg"),
        max("music_max_freq").alias("music_freq_agg"),
        
        // Complex aggregation to get user ID associated with max frequency for each topic
        expr("max_by(games_max_user, games_max_freq)").alias("games_user_agg"),
        expr("max_by(movies_max_user, movies_max_freq)").alias("movies_user_agg"),
        expr("max_by(music_max_user, music_max_freq)").alias("music_user_agg")
      )
      
      // Join with current recommendation tracker to update max values
      val currentRecDF = recTrackerDF.join(
        recAggrDF,
        recTrackerDF("user") === recAggrDF("followee"),
        "left_outer"
      )
      
      // Update recommendation tracker with new max values
      recTrackerDF = currentRecDF.select(
        $"user",
        // For each topic, update max freq if new max is higher
        when($"games_freq_agg" > $"games_max_freq" && $"games_interest", $"games_freq_agg")
          .otherwise($"games_max_freq").as("games_max_freq"),
        when($"movies_freq_agg" > $"movies_max_freq" && $"movies_interest", $"movies_freq_agg")
          .otherwise($"movies_max_freq").as("movies_max_freq"),
        when($"music_freq_agg" > $"music_max_freq" && $"music_interest", $"music_freq_agg")
          .otherwise($"music_max_freq").as("music_max_freq"),
        
        // Update user IDs associated with max frequencies
        when($"games_freq_agg" > $"games_max_freq" && $"games_interest", $"games_user_agg")
          .otherwise($"games_max_user").as("games_max_user"),
        when($"movies_freq_agg" > $"movies_max_freq" && $"movies_interest", $"movies_user_agg")
          .otherwise($"movies_max_user").as("movies_max_user"),
        when($"music_freq_agg" > $"music_max_freq" && $"music_interest", $"music_user_agg")
          .otherwise($"music_max_user").as("music_max_user"),
        
        // Retain interest flags and original frequencies
        $"games_interest", $"movies_interest", $"music_interest",
        $"games_freq", $"movies_freq", $"music_freq"
      )
      
      // Optionally cache every few iterations if memory allows
      if (i % 3 == 0 && i < iterations - 1) {
        newRanksDF.cache()
        recTrackerDF.cache()
      }
      
      // Update ranks for next iteration
      ranksDF = newRanksDF
    }
    
    // Step 6: Save the final PageRank results in the required format
    ranksDF.select($"user".cast(StringType), $"rank".cast(StringType))
      .map(row => row.getString(0) + "\t" + row.getString(1))
      .write.text(pageRankOutputPath)
    
    // ----------------------------------------------------------------------
    // NEW CODE: Generate and save final recommendations
    // ----------------------------------------------------------------------
    // For each user, determine the highest frequency topic and its corresponding source user
    val finalRecsDF = recTrackerDF
      // Create a unified view of all topics and their max frequencies
      .select(
        $"user",
        // Create arrays of topic-maxfreq-maxuser tuples for each interest
        array(
          when($"games_interest", 
               struct(lit("games").as("topic"), $"games_max_freq".as("freq"), $"games_max_user".as("rec_user")))
              .otherwise(null),
          when($"movies_interest", 
               struct(lit("movies").as("topic"), $"movies_max_freq".as("freq"), $"movies_max_user".as("rec_user")))
              .otherwise(null),
          when($"music_interest", 
               struct(lit("music").as("topic"), $"music_max_freq".as("freq"), $"music_max_user".as("rec_user")))
              .otherwise(null)
        ).as("interests")
      )
      // Explode the array to get one row per interest
      .select($"user", explode($"interests").as("interest"))
      .filter($"interest".isNotNull) // Remove null interests
      .select($"user", $"interest.topic", $"interest.freq", $"interest.rec_user")
      
      // For each user, find the interest with the maximum frequency
      .withColumn("row_num", row_number().over(
        Window.partitionBy("user").orderBy($"freq".desc, $"rec_user".desc)
      ))
      .filter($"row_num" === 1)
      .select($"user", $"rec_user", $"freq")
    
    // Save recommendations to output path in the required format
    finalRecsDF
      .select($"user".cast(StringType), $"rec_user".cast(StringType), $"freq".cast(StringType))
      .map(row => row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2))
      .write.text(recsOutputPath)
    
    // Clean up cached DataFrames
    outgoingLinksDF.unpersist()
    topicsDF.unpersist()
    userInterestsDF.unpersist()
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