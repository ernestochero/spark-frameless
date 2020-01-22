package framelessOverview

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import frameless.functions.aggregate._
import frameless.TypedDataset
import frameless.syntax._
object TypedDatasetOverview extends App {
  val conf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Frameless repl")
    .set("spark.ui.enable", "false")

  implicit val spark = SparkSession
    .builder()
    .config(conf)
    .appName("REPL")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._
  // "LocationID","Borough","Zone","service_zone"
  case class TaxiZone(LocationId: Int, Borough: String, Zone: String, ServiceZone: String)
  val taxiZoneDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val taxiZoneDataSet = taxiZoneDataFrame
    .select(col("LocationID"), col("Borough"), col("Zone"), col("service_zone").as("ServiceZone"))
    .as[TaxiZone]

  val taxiZoneTypedDataSet: TypedDataset[TaxiZone] = TypedDataset.create(taxiZoneDataSet)
  val zonesTyped = for {
    count <- taxiZoneTypedDataSet.count()
    zones <- taxiZoneTypedDataSet.take((count / 2).toInt)
  } yield zones

  zonesTyped.run().foreach(println(_))

  case class Bar(d: Double, s: String)
  case class Foo(i: Int, b: Bar)
  val ds: TypedDataset[Foo] = TypedDataset.create(Seq(Foo(1, Bar(1.1, "s"))))

  case class Apartment(city: String, surface: Int, price: Double, bedrooms: Int)
  val apartments = Seq(
    Apartment("Paris", 50, 300000.0, 2),
    Apartment("Paris", 100, 450000.0, 3),
    Apartment("Paris", 25, 250000.0, 1),
    Apartment("Lyon", 83, 200000.0, 2),
    Apartment("Lyon", 45, 133000.0, 1),
    Apartment("Nice", 74, 325000.0, 3)
  )

}
