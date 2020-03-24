import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  var spark: SparkSession = _
  val appName: String = "spark-test"
  val master: String = "local[4]"
  val properties: Map[String, String] = Map()
  final val testPortRange: Range = 50000 until 60000
  final val rnd: scala.util.Random = new scala.util.Random

  def nextTestPort: Int = {
    testPortRange.start + rnd.nextInt(testPortRange.length)
  }


  def config(appID: String): SparkConf = {
    new SparkConf(false)
      .setAppName(appName + ": " + appID)
      .setMaster(master)
      .set("spark.driver.port", nextTestPort.toString)
      .set("spark.ui.enabled", "false")
  }

  def setupSparkSession(sparkName: String): Unit = {
    var optSc: Option[SparkSession] = None
    while (optSc.isEmpty) {
      val conf = config(sparkName)
      properties.foreach(kv => conf.set(kv._1, kv._2))
      try {
        optSc = Some(SparkSession.builder.config(conf)
          .config("hadoop.shell.missing.defaultFs.warning", "false")
          .getOrCreate())
      } catch {
        case e: Throwable => throw e
      }
    }
    spark = optSc.get
  }


}

