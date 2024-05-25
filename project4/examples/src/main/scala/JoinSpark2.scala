import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

@SerialVersionUID(123L)
case class Employee ( name: String, dno: Int, address: String )
      extends Serializable {}

@SerialVersionUID(123L)
case class Department ( name: String, dno: Int )
      extends Serializable {}

object JoinSpark2 {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("Join")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    val e = sc.textFile(args(0)).map( line => { val a = line.split(",")
                                                Employee(a(0),a(1).toInt,a(2)) } )
    val d = sc.textFile(args(1)).map( line => { val a = line.split(",")
                                                Department(a(0),a(1).toInt) } )
    val res = e.map( e => (e.dno,e) ).join(d.map( d => (d.dno,d) ))
                .map { case (k,(e,d)) => e.name+" "+d.name }
    res.saveAsTextFile(args(2))
    sc.stop()
  }
}
