//Name : Prem Atul Jethwa
//UTA ID : 1001861810

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.split

object Netflix {
  def main ( args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("Netflix")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import org.apache.spark.sql.functions.col

    val v1 = spark.read.option("delimiter", ";").csv(args(0))

    val v2 = v1.filter(col("_c0").contains(","))

    val df = v2.select(functions.split(col("_c0"),",").getItem(0).as("USERID"),
      split(col("_c0"),",").getItem(1)as("RATING"),
      split(col("_c0"),",").getItem(2)as("DATE")).drop("_c0")

    df.createOrReplaceTempView("USERANDRATING")

    val AVGUSERRATINGDF = spark.sql("SELECT USERID, substring(AVG(RATING),0, instr(AVG(RATING),'.')+1) as AVGRATING  FROM USERANDRATING group by USERID")
    AVGUSERRATINGDF.createOrReplaceTempView("AVGRATINGANDCOUNT")

    val AVGRATINGANDCOUNTDF = spark.sql("SELECT AVGRATING as RATING, COUNT(AVGRATING) as countRating FROM AVGRATINGANDCOUNT group by AVGRATING order by AVGRATING")
    AVGRATINGANDCOUNTDF.show(41)

  }

}