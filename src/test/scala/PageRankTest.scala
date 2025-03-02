import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSpec, MustMatchers}

class PageRankTest
  extends FunSpec
  with TestingUtil
  with MustMatchers
  with LocalSparkSession
  with BeforeAndAfterEach {

  val Iterations = 10
  val InputGraphPath = "data/SmallGraph1"
  val InputTopicsPath = "data/SmallGraph1-Topics"
  val OutputGraphPath = "data/SmallGraph1-Output"
  val OutputRecsPath = "data/SmallGraph1-OutputRecs"
  val ReferencePath = "data/SmallGraph1-Reference"
  val ReferenceRecsPath = "data/SmallGraph1-RecsRef"

  /**
   * This function gets call before the execution of each test.
   * If adding tests of your own, make sure to delete your files as well.
   */
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    FileUtils.deleteQuietly(new File(OutputGraphPath))
    FileUtils.deleteQuietly(new File(OutputRecsPath))
  }

  describe("the PageRank algorithm") {
    it("should match the expected output for SmallGraph1") {
      PageRank.calculatePageRank(InputGraphPath, InputTopicsPath, OutputGraphPath, OutputRecsPath, Iterations, spark)
      comparePageRanks(OutputGraphPath, ReferencePath)
      compareRecs(OutputRecsPath, ReferenceRecsPath)
    }
  }
}
