import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object JoinSpark {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(conf)
    val e = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                (a(0),a(1).toInt,a(2)) } )
    val d = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                (a(0),a(1).toInt) } )
    val res = e.map( e => (e._2,e) ).join(d.map( d => (d._2,d) ))
                .map { case (k,(e,d)) => e._1+" "+d._1 }
    res.saveAsTextFile(args(2))
    sc.stop()
  }
}
