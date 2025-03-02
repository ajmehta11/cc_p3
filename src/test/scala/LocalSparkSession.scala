import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, MustMatchers, Suite}

/**
  * Trait that creates a SparkSession for testing.
  * A class needs only extend this trait.
  */
trait LocalSparkSession extends BeforeAndAfterAll with MustMatchers { self: Suite =>

  @transient var spark: SparkSession = _
  @transient var sc: SparkContext = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val sessionBuilder = SparkSession.builder()
    spark = sessionBuilder.config(getConf).getOrCreate()
    sc = spark.sparkContext
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    resetSparkSession()
  }

  def resetSparkSession(): Unit = {
    this.stop(spark)
    spark = null
    sc = null
  }

  def getConf: SparkConf = {
    val conf = new SparkConf(true)
    conf.setMaster("local[*]")
    conf.setAppName("unit-tests")
    conf.set("spark.ui.enabled", "false")
    conf.set("spark.driver.allowMultipleContexts", "true")
    conf
  }

  def stop(sparkSession: SparkSession) {
    if (sparkSession != null)
      sparkSession.stop()
    // To avoid RPC rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
  }

  /**
    * Asserts that two RDDs are equal or not.
    * It is order sensitive - two RDDs will not match unless the content and order are the same.
    * @param one one of the RDDs.
    * @param two the other
    * @tparam T the type of the RDD.
    */
  def assertEquals[T](one: RDD[T], two: RDD[T]): Unit = {
    val n = one.count()

    n must be(two.count())

    val oneRows = one.take(n.toInt)
    val twoRows = two.take(n.toInt)

    oneRows.zip(twoRows).foreach {
      case (row1, row2) =>
        row1 mustEqual row2
    }
  }

}
