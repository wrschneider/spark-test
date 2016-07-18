package bill

import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.holdenkarau.spark.testing.SharedSparkContext

class TransformTest extends FlatSpec with Matchers with SharedSparkContext {
  "Transformer" should "add column to dataframe" in {
    val sqlContext = new SQLContext(sc)
    val rows = Seq[Row](
        Row(1),
        Row(2),
        Row(3)
        ).asJava
    val schema = StructType(Seq(StructField("a", IntegerType)))
   
    val df = sqlContext.createDataFrame(rows, schema)
    val df2 = new Transform().addCol(df)
    assert(df2.count() > 0)
    assert(df2.agg(sum("a")).first.getLong(0) == 6)
    assert(df2.agg(sum("b")).first.getLong(0) == 21)
  }
}