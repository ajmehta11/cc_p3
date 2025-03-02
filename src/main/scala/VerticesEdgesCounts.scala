import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object VerticesEdgesCounts {

  /**
    * Computes the vertices and nodes count.
    *
    * Note: you cannot assume that the graph does not have repeated edges. The following is a valid
    * graph, and your code should properly count the distinct vertices and nodes.
    *
    *   user_1   user_2
    *   user_2   user_3
    *   user_1   user_2
    *
    * Your function should return the result of VerticesEdgesCounts#countsToString with the actual
    * values.
    *
    * @param inputPath the path to the input graph.
    * @param spark the SparkSession. Note that it is provided for convenience, you might not
    *              need it if you use the RDD API.
    * @param sc the SparkContext. Note that it is provided for convenience, you might not
    * *         need it if you use the DataFrame API.
    * @return the result of calling VerticesEdgesCounts#countsToString with the vertices and nodes
    *         count.
    */
  def verticesAndNodesCount(inputPath: String, spark: SparkSession, sc: SparkContext): String = {
    // TODO: copy your code from the notebook here.
    // TODO: replace with the actual values. You should not hardcode them as the grader tests the
    //  function on a secret dataset.
    val graphRDD = sc.textFile(inputPath)
    val edges_graph = graphRDD.map(.split("\\s+")).filter(.length >= 2).map(arr => (arr(0), arr(1)))
    val edges = edges.distinct().count()
    val vertices_graph = edges.flatMap { case (src, dst) => Seq(src, dst) }
    val vertices = vertices.distinct().count()

    countsToString(vertices, edges)
  }

  /**
   * @param args it should be called with two arguments, the input path, and the output path.
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.sparkSession()
    val sc = spark.sparkContext

    val inputPath = args(0)
    val outputPath = args(1)

    val outputString = verticesAndNodesCount(inputPath, spark, sc)
    saveToFile(outputString, outputPath)
  }

  /**
    * Formats the vertices and edges counts in the format expected by the grader.
    * @param numVertices the number of vertices.
    * @param numEdges the number of edges.
    * @return a string with the vertices and edges counts in the format expected by the grader.
    */
  def countsToString(numVertices: Long, numEdges: Long): String =
    s"""|num_vertices=$numVertices
        |num_edges=$numEdges""".stripMargin

  /**
   * Saves the output string to the output path.
   * @param outputString the string to save.
   * @param outputPath the file to save to.
   */
  def saveToFile(outputString: String, outputPath: String): Unit =
    Files.write(Paths.get(outputPath), outputString.getBytes(StandardCharsets.UTF_8))

}
