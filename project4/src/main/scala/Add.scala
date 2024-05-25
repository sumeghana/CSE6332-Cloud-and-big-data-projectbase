import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {

  case class Block(rows: Int, columns: Int, data: Array[Double]) {
  override def toString: String = {
    var s = ""
    for (i <- 0 until rows) {
      for (j <- 0 until columns) {
        s += f" ${data(i * columns + j)}%.2f"
      }
      // Add a newline at the end of each row, but avoid an extra newline after the last row
      if (i < rows - 1) {
        s += "\n"
      }
    }
    s
  }
}



  def toBlock(rows: Int, columns: Int, triples: Iterable[(Int, Int, Double)]): Block = {
    val data = Array.fill[Double](rows * columns)(0)
    triples.foreach {
      case (i, j, v) =>
        val index = (i % rows) * columns + (j % columns)
        data(index) = v
    }
    Block(rows, columns, data)
  }

  def blockAdd(m: Block, n: Block): Block = {
    val data = m.data.zip(n.data).map { case (mVal, nVal) => mVal + nVal }
    Block(m.rows, m.columns, data)
  }

  def createBlockMatrix(sc: SparkContext, file: String, rows: Int, columns: Int): RDD[((Int, Int), Block)] = {
    sc.textFile(file)
      .map(_.split(",").map(_.trim).toList match {
        case i :: j :: v :: Nil => (i.toInt, j.toInt, v.toDouble)
        case _ => throw new IllegalArgumentException("Invalid input format")
      })
      .groupBy { case (i, j, _) => (i / rows, j / columns) }
      .mapValues(triples => toBlock(rows, columns, triples))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Block Matrix Addition")
      // Removed the Kryo registrator setting
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.shuffle.partitions", "200") // Adjust based on your data size and cluster
      .set("spark.executor.memory", "2g") // Adjust based on your cluster setup
      .set("spark.executor.memoryOverhead", "600m") // Adjust based on your cluster setup
      // Removed speculative execution to simplify configuration

    val sc = new SparkContext(conf)

    val rows = args(0).toInt
    val columns = args(1).toInt
    val mFile = args(2)
    val nFile = args(3)

    val m = createBlockMatrix(sc, mFile, rows, columns)
    val n = createBlockMatrix(sc, nFile, rows, columns)

    val result = m.join(n).mapValues { case (mBlock, nBlock) =>
      blockAdd(mBlock, nBlock)
    }

    // Filter and print the specific block with coordinates (1,2)
    result.filter { case (coord, _) => coord == (1, 2) }.collect().foreach {
    case (coord, block) => 
     println(s"($coord,") // Start block coordinates
     println(block.toString) // Print block data directly
     println(")") // Close with a parenthesis on a new line after the block data
   }

  }
}

