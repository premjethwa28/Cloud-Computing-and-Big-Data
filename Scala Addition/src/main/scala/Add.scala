import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  val rows = 100
  val columns = 100

  case class Block ( data: Array[Double] ) {
    override
    def toString (): String = {
      var string = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          string += "\t%.3f".format(data(i*rows+j))
        string += "\n"
      }
      string
    }
  }

  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( triples: List[(Int,Int,Double)] ): Block = {
  var triple_array:Array[Double] = new Array[Double](rows*columns)
    triples.foreach(triple=> {
      triple_array(triple._1*rows+triple._2) = triple._3
    })
    new Block(triple_array)

  }

  /* Add two Blocks */
  def blockAdd ( m_block: Block, n_block: Block ): Block = {
  var triple_array:Array[Double] = new Array[Double](rows*columns)
    for ( i <- 0 until rows ) {
      for ( j <- 0 until columns )
        triple_array(i*rows+j) = m_block.data(i*rows+j) + n_block.data(i*rows+j)
    }
    new Block(triple_array)

  }

  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( scan: SparkContext, file: String ): RDD[((Int,Int),Block)] = {
  scan.textFile(file).map( line => { val l_s = line.split(",")
      ((l_s(0).toInt/rows, l_s(1).toInt/columns), (l_s(0).toInt%rows, l_s(1).toInt%columns, l_s(2).toDouble)) } )
      .groupByKey().map{ case (key,value) => (key, toBlock(value.toList)) }

  }

  def main ( args: Array[String] ) {
    val main_conf = new SparkConf().setAppName("Spark_Add")
    val config_spark_add = new SparkContext(main_conf)
    val Spark_M_Input= createBlockMatrix(config_spark_add , args(0))
    val Spark_n_Input=  createBlockMatrix(config_spark_add , args(1))
    val Addition_result = Spark_M_Input.join(Spark_n_Input).map{ case(key, (m_block, n_block)) => (key, blockAdd(m_block, n_block)) }
    Addition_result.saveAsTextFile(args(2))
    config_spark_add.stop()

  }
}