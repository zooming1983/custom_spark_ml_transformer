import java.io.{ByteArrayInputStream, File}

import javax.imageio.ImageIO
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}


class CustomImageTransformer(override val uid: String) extends Transformer {

  final val inputCol = new Param[String](this, "image", "image")
  final val outputCol = new Param[String](this, "width", "width column");

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)
  setDefault(inputCol -> "image", outputCol -> "width")

  final def getOutputCol: String = $(outputCol)


  override def transformSchema(schema: StructType): StructType = {
    val actualDataType = schema($(inputCol)).dataType
    require(actualDataType.equals(DataTypes.StringType),
      s"Column ${$(inputCol)} must be BinaryType but was actually $actualDataType.")

    schema.add(StructField($(outputCol), IntegerType, nullable = true))

  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)


  def transform(df: Dataset[_]): DataFrame = {
    val getWidthUdf = udf { in: Array[Byte] => ImageIO.read(new ByteArrayInputStream(in)).getWidth }
    df.select(col("*"), getWidthUdf(df.col($(inputCol))).as($(outputCol)))
  }


}
