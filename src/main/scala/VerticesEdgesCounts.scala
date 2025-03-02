import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object VerticesEdgesCounts {

  /**
    * Computes the vertices and nodes count.
    *
    * @param inputPath the path to the input graph.
    * @param spark the SparkSession.
    * @param sc the SparkContext.
    * @return the result of calling VerticesEdgesCounts#countsToString with the vertices and edges count.
    */
  def verticesAndNodesCount(inputPath: String, spark: SparkSession, sc: SparkContext): String = {
    val graphRDD = sc.textFile(inputPath)

    // Extract edges
    val edges_graph = graphRDD
      .map(line => line.split("\\s+"))
      .filter(arr => arr.length >= 2)
      .map(arr => (arr(0), arr(1)))

    // Count distinct edges
    val distinctEdgesCount = edges_graph.distinct().count()

    // Extract vertices and count distinct ones
    val vertices_graph = edges_graph.flatMap { case (src, dst) => Seq(src, dst) }
    val distinctVerticesCount = vertices_graph.distinct().count()

    countsToString(distinctVerticesCount, distinctEdgesCount)
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
    * Formats the vertices and edges counts in the expected format.
    * @param numVertices the number of vertices.
    * @param numEdges the number of edges.
    * @return a formatted string.
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
