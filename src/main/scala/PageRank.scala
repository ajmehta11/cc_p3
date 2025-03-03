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
    * The graph topics file is a plain text file with four tab-separated columns:
    *   user_id   games   movies   music
    * where the three topic frequencies sum to 10.
    *
    * After calculating the page ranks of all the nodes in the graph,
    * the output should be written to `pageRankOutputPath` in the following format:
    *
    *   node  rank
    *   node  rank
    *   node  rank
    *
    * Additionally, a recommendation output file should be written to
    * `recsOutputPath` in the format:
    *
    *   [user_id] \t [recommended_user_id] \t [post_frequency]
    *
    * @param inputGraphPath path of the input graph.
    * @param graphTopicsPath path of the topics file.
    * @param pageRankOutputPath path of the output of page rank.
    * @param recsOutputPath path of the output recommendations.
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

    // ---------------------------
    // Step 1: Load the graph data
    // ---------------------------
    val graphDF = spark.read.text(inputGraphPath)
      .map(row => {
        val parts = row.getString(0).split("\t")
        // Parse user IDs as Long for better performance
        (parts(0).toLong, parts(1).toLong)
      }).toDF("follower", "followee")

    // Optimization: Calculate a good partition number based on data size
    val numPartitions = 200  // Adjust based on cluster size

    // -------------------------------
    // Step 2: Identify all unique users
    // -------------------------------
    val allUsersDF = graphDF.select($"follower".alias("user"))
      .union(graphDF.select($"followee".alias("user")))
      .distinct()
      .repartition(numPartitions)

    val numUsers = allUsersDF.count()

    // -------------------------------------------------------
    // Step X: Load Topics Data and Initialize Recommendation Data
    // -------------------------------------------------------
    // Define schema for topics file (user, games, movies, music)
    val topicsSchema = StructType(Array(
      StructField("user", LongType, nullable = false),
      StructField("games", DoubleType, nullable = false),
      StructField("movies", DoubleType, nullable = false),
      StructField("music", DoubleType, nullable = false)
    ))

    val topicsDF = spark.read
      .option("delimiter", "\t")
      .schema(topicsSchema)
      .csv(graphTopicsPath)

    // Join topics with all users (if some users do not appear, fill with 0)
    val allUsersWithTopicsDF = allUsersDF.join(topicsDF, Seq("user"), "left_outer")
      .na.fill(0.0, Seq("games", "movies", "music"))

    // Create interest flags (a user is interested if frequency >= 3.0)
    // Also initialize recommendation variables: for each topic, the current best frequency is the user’s own posting frequency
    // and the originating user is itself.
    var recsDF = allUsersWithTopicsDF
      .withColumn("interested_games", $"games" >= 3.0)
      .withColumn("interested_movies", $"movies" >= 3.0)
      .withColumn("interested_music", $"music" >= 3.0)
      .withColumn("rec_games_freq", $"games")
      .withColumn("rec_games_src", $"user")
      .withColumn("rec_movies_freq", $"movies")
      .withColumn("rec_movies_src", $"user")
      .withColumn("rec_music_freq", $"music")
      .withColumn("rec_music_src", $"user")
      .select("user", "interested_games", "interested_movies", "interested_music",
              "rec_games_freq", "rec_games_src",
              "rec_movies_freq", "rec_movies_src",
              "rec_music_freq", "rec_music_src")
      .repartition(numPartitions, $"user")

    // -------------------------------
    // Step 3: Initialize PageRank scores
    // -------------------------------
    var ranksDF = allUsersDF.withColumn("rank", lit(1.0 / numUsers))

    // -------------------------------
    // Step 4: Compute Outgoing Links
    // -------------------------------
    val outgoingLinksDF = graphDF.groupBy("follower")
      .agg(collect_list("followee").alias("followees"),
           count("followee").alias("numFollowing"))
      .repartition(numPartitions, $"follower")
      .cache()

    // -------------------------------
    // Step 5: Iterative Computation (PageRank and Recommendations)
    // -------------------------------
    for (i <- 1 to iterations) {
      // Repartition ranksDF for join efficiency if needed
      if (i > 1) {
        ranksDF = ranksDF.repartition(numPartitions, $"user")
      }

      // --- PageRank update ---
      // Calculate contributions from each user to their followees
      val contributionsDF = ranksDF.join(
        outgoingLinksDF,
        ranksDF("user") === outgoingLinksDF("follower"),
        "inner"
      )
      .withColumn("contribution", $"rank" / $"numFollowing")
      .select($"followees", $"contribution")
      .withColumn("followee", explode($"followees"))
      .select($"followee", $"contribution")

      val repartitionedContributionsDF = contributionsDF.repartition(numPartitions, $"followee")

      val summedContributionsDF = repartitionedContributionsDF
        .groupBy("followee")
        .agg(sum("contribution").alias("received_contrib"))

      // Identify dangling nodes (users with no outgoing links)
      val danglingNodesDF = ranksDF.join(
        outgoingLinksDF.select("follower"),
        ranksDF("user") === outgoingLinksDF("follower"),
        "left_anti"
      )

      val danglingMass = danglingNodesDF.agg(sum("rank")).first().getDouble(0)
      val danglingRedistribution = danglingMass / numUsers

      // Compute new ranks with the PageRank formula
      val newRanksDF = allUsersDF.join(
          summedContributionsDF,
          allUsersDF("user") === summedContributionsDF("followee"),
          "left_outer"
        )
        .withColumn("rank",
          lit((1.0 - dampingFactor) / numUsers) +
          lit(dampingFactor) * (coalesce($"received_contrib", lit(0.0)) + lit(danglingRedistribution))
        )
        .select($"user", $"rank")

      // --- Recommendation propagation update ---
      // Each user sends its current recommendation values to its followees,
      // but only for topics it is interested in.
      val recMessagesDF = recsDF.join(
          graphDF, recsDF("user") === graphDF("follower"), "inner"
      ).select(
          graphDF("followee").alias("user"),
          when($"interested_games", $"rec_games_freq").otherwise(lit(0.0)).alias("msg_games_freq"),
          when($"interested_games", $"rec_games_src").otherwise(lit(0L)).alias("msg_games_src"),
          when($"interested_movies", $"rec_movies_freq").otherwise(lit(0.0)).alias("msg_movies_freq"),
          when($"interested_movies", $"rec_movies_src").otherwise(lit(0L)).alias("msg_movies_src"),
          when($"interested_music", $"rec_music_freq").otherwise(lit(0.0)).alias("msg_music_freq"),
          when($"interested_music", $"rec_music_src").otherwise(lit(0L)).alias("msg_music_src")
      )

      // Aggregate messages per receiver by taking the max contribution for each topic.
      // The struct comparison will use lexicographical order (frequency first, then sender id) to break ties.
      val aggRecsDF = recMessagesDF.groupBy("user")
        .agg(
          max(struct($"msg_games_freq".alias("freq"), $"msg_games_src".alias("src"))).alias("agg_games"),
          max(struct($"msg_movies_freq".alias("freq"), $"msg_movies_src".alias("src"))).alias("agg_movies"),
          max(struct($"msg_music_freq".alias("freq"), $"msg_music_src".alias("src"))).alias("agg_music")
        )

      // For each user, update recommendation values by comparing current values with aggregated incoming messages.
      // If the incoming value is greater—or equal with a higher source id—update the recommendation.
      val updatedRecsDF = recsDF.join(aggRecsDF, Seq("user"), "left_outer")
        .select(
          recsDF("user"),
          recsDF("interested_games"),
          recsDF("interested_movies"),
          recsDF("interested_music"),
          when(
            $"agg_games.freq".isNotNull &&
            ( ($"agg_games.freq" > $"rec_games_freq") ||
              ( ($"agg_games.freq" === $"rec_games_freq") && ($"agg_games.src" > $"rec_games_src") )
            ),
            $"agg_games.freq"
          ).otherwise($"rec_games_freq").alias("rec_games_freq"),
          when(
            $"agg_games.freq".isNotNull &&
            ( ($"agg_games.freq" > $"rec_games_freq") ||
              ( ($"agg_games.freq" === $"rec_games_freq") && ($"agg_games.src" > $"rec_games_src") )
            ),
            $"agg_games.src"
          ).otherwise($"rec_games_src").alias("rec_games_src"),
          when(
            $"agg_movies.freq".isNotNull &&
            ( ($"agg_movies.freq" > $"rec_movies_freq") ||
              ( ($"agg_movies.freq" === $"rec_movies_freq") && ($"agg_movies.src" > $"rec_movies_src") )
            ),
            $"agg_movies.freq"
          ).otherwise($"rec_movies_freq").alias("rec_movies_freq"),
          when(
            $"agg_movies.freq".isNotNull &&
            ( ($"agg_movies.freq" > $"rec_movies_freq") ||
              ( ($"agg_movies.freq" === $"rec_movies_freq") && ($"agg_movies.src" > $"rec_movies_src") )
            ),
            $"agg_movies.src"
          ).otherwise($"rec_movies_src").alias("rec_movies_src"),
          when(
            $"agg_music.freq".isNotNull &&
            ( ($"agg_music.freq" > $"rec_music_freq") ||
              ( ($"agg_music.freq" === $"rec_music_freq") && ($"agg_music.src" > $"rec_music_src") )
            ),
            $"agg_music.freq"
          ).otherwise($"rec_music_freq").alias("rec_music_freq"),
          when(
            $"agg_music.freq".isNotNull &&
            ( ($"agg_music.freq" > $"rec_music_freq") ||
              ( ($"agg_music.freq" === $"rec_music_freq") && ($"agg_music.src" > $"rec_music_src") )
            ),
            $"agg_music.src"
          ).otherwise($"rec_music_src").alias("rec_music_src")
        )

      // Update recsDF for the next iteration.
      recsDF = updatedRecsDF.repartition(numPartitions, $"user")

      // Update ranksDF for the next iteration.
      ranksDF = newRanksDF

      // Optionally cache intermediate DataFrames every few iterations.
      if (i % 3 == 0 && i < iterations - 1) {
        newRanksDF.cache()
      }
    }

    // -------------------------------
    // Step 6: Write out the final PageRank results
    // -------------------------------
    ranksDF.select($"user".cast(StringType), $"rank".cast(StringType))
      .map(row => row.getString(0) + "\t" + row.getString(1))
      .write.text(pageRankOutputPath)

    // -------------------------------
    // Step 7: Final Recommendation Selection
    // -------------------------------
    // For each user, choose the recommendation based on the topic for which the propagated posting frequency is highest.
    // Ties are broken by the higher originating user id.
    // First, for each topic, if the user is interested then use the recommendation; otherwise, treat it as zero.
    val finalRecsDF = recsDF.withColumn("final_rec_games", 
                              when($"interested_games", struct($"rec_games_freq".alias("freq"), $"rec_games_src".alias("src")))
                              .otherwise(struct(lit(0.0).alias("freq"), lit(0L).alias("src"))))
      .withColumn("final_rec_movies", 
                              when($"interested_movies", struct($"rec_movies_freq".alias("freq"), $"rec_movies_src".alias("src")))
                              .otherwise(struct(lit(0.0).alias("freq"), lit(0L).alias("src"))))
      .withColumn("final_rec_music", 
                              when($"interested_music", struct($"rec_music_freq".alias("freq"), $"rec_music_src".alias("src")))
                              .otherwise(struct(lit(0.0).alias("freq"), lit(0L).alias("src"))))
      .withColumn("final_rec",
         when( ($"final_rec_games.freq" >= $"final_rec_movies.freq") && ($"final_rec_games.freq" >= $"final_rec_music.freq"),
               $"final_rec_games")
         .when( ($"final_rec_movies.freq" >= $"final_rec_games.freq") && ($"final_rec_movies.freq" >= $"final_rec_music.freq"),
               $"final_rec_movies")
         .otherwise($"final_rec_music")
      )

    val recommendations = finalRecsDF.select($"user", 
                                               $"final_rec.src".alias("recommended_user"), 
                                               $"final_rec.freq".alias("post_frequency"))
    
    // Write recommendations to file in the required format.
    recommendations.select($"user".cast(StringType), $"recommended_user".cast(StringType), $"post_frequency".cast(StringType))
      .map(row => row.getString(0) + "\t" + row.getString(1) + "\t" + row.getString(2))
      .write.text(recsOutputPath)

    // Clean up cached DataFrames.
    outgoingLinksDF.unpersist()
  }

  /**
    * @param args it should be called with four arguments:
    *             [inputGraphPath] [graphTopicsPath] [pageRankOutputPath] [recsOutputPath]
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
