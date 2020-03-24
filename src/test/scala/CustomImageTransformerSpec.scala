import org.scalatest.funsuite.AnyFunSuite

class CustomImageTransformerSpec extends AnyFunSuite with SparkSessionProvider {

  test("custom ML Transformer ") {
    setupSparkSession("custom_ml_transformer")
    val sparkSession = spark
    import sparkSession.implicits._
    val df = sparkSession.sparkContext.binaryFiles("src/test/data/").map { case (fileName, pds) => {
      pds.toArray()
    }
    }.toDF("image")

    val myTransformer = new CustomImageTransformer("myTest")
    val tf = myTransformer.transform(df)
    tf.show()

  }





}
