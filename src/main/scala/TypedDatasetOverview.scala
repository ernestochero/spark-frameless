package framelessOverview

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import frameless.functions._
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
  case class TaxiZone(locationId: Int, borough: String, zone: String, serviceZone: String)
  val taxiZoneDataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val taxiZoneDataSet = taxiZoneDataFrame
    .select(col("LocationID").as("locationId"),
            col("Borough").as("borough"),
            col("Zone").as("zone"),
            col("service_zone").as("serviceZone"))
    .as[TaxiZone]

  val tzTypedDS: TypedDataset[TaxiZone] = TypedDataset.create(taxiZoneDataSet)
  val zonesTyped = for {
    count <- tzTypedDS.count()
    zones <- tzTypedDS.take((count / 2).toInt)
    zonesUpperCase = zones.map(tz => tz.copy(zone = tz.zone.toUpperCase))
  } yield zonesUpperCase
  zonesTyped.run().foreach(println(_))

  case class BoroughWithZone(borough: String, zone: String, random: Int)

  val resultTyped: TypedDataset[BoroughWithZone] =
    tzTypedDS
      .select(tzTypedDS('borough),
              nonAggregate.concat(tzTypedDS('locationId).cast[String], tzTypedDS('zone)),
              frameless.functions.lit(1000))
      .as[BoroughWithZone]()
  resultTyped.show().run()

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
  val aptds: TypedDataset[Apartment] = TypedDataset.create(apartments)

  case class ApartmentDetails(city: String, price: Double, surface: Int, ratio: Double)
  val aptWithRatio = aptds
    .select(
      aptds('city),
      aptds('price),
      aptds('surface),
      aptds('price) / aptds('surface).cast[Double]
    )
    .as[ApartmentDetails]()

  case class CityRatio(city: String, ratio: Double)
  val cityRatio: TypedDataset[CityRatio] = aptWithRatio.project[CityRatio]
  cityRatio.show().run()

  case class PriceInfo(ratio: Double, price: Double)
  val priceInfo: TypedDataset[PriceInfo] = aptWithRatio.project[PriceInfo]
  priceInfo.show().run()

  case class ApartmentShortInfo(city: String, price: Double, bedrooms: Int)
  val aptTypedDs2: TypedDataset[ApartmentShortInfo] = aptds.project[ApartmentShortInfo]
  aptTypedDs2.union(aptds).show().run()

  aptTypedDs2
    .withColumnTupled(
      frameless.functions.nonAggregate
        .when(aptTypedDs2('city) === "Paris", aptTypedDs2('price))
        .when(aptTypedDs2('city) === "Lyon", frameless.functions.lit(1.1))
        .otherwise(frameless.functions.lit(0.0))
    )
    .show()
    .run()

  val t = cityRatio.select(cityRatio('city), lit(List("abc", "c", "d")))
  t.withColumnTupled(
      frameless.functions.nonAggregate.arrayContains(t('_2), " abc")
    )
    .show()
    .run()
}
