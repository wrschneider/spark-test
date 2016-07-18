package bill

import org.apache.spark.sql.DataFrame

class Transform {
  def addCol(df: DataFrame) = {
    // take df col 'a' and add 5 
    df.withColumn("b", df.col("a").plus(5))
  }
}
