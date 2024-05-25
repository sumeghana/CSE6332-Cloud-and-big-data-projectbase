import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Trip {
  case class TripData(distance: Int, amount: Float)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Trip")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // Reading the data from a CSV file, skipping the first line (header)
    val input = sc.textFile(args(0))
    val data = input.filter(!_.startsWith("VendorID")) // Assuming 'VendorID' is the first column header

    // Mapping to TripData
    val tripData = data.map(line => {
      val parts = line.split(",")
      val tripDistance = Math.round(parts(4).toFloat)
      val totalAmount = parts.last.toFloat
      TripData(tripDistance, totalAmount)
    }).filter(_.distance < 200).toDS()

    // Registering the dataset as a temporary table
    tripData.createOrReplaceTempView("tripData")

    // SQL query to calculate the average amount for each rounded distance, filtering out groups with less than 3 trips
    val query = spark.sql(
      """SELECT distance, AVG(amount) AS avg_amount
        |FROM tripData
        |GROUP BY distance
        |HAVING COUNT(*) > 2
        |ORDER BY distance ASC""".stripMargin)

    // Printing the results
    query.collect.foreach {
      case Row(distance: Int, avg_amount: Double) =>
        println(f"$distance%3d\t$avg_amount%3.3f")
    }

    spark.stop()
  }
}
