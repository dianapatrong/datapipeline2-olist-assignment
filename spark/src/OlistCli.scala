import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OlistCli {

    def listDelayedDeliveries(spark: SparkSession) = {
        var df = spark.read.option("header", true).csv("data/archive/olist_orders_dataset.csv")
        df = df.withColumn("delivery_delay(days)", datediff(to_utc_timestamp(df.col("order_delivered_customer_date"), "America/Sao_Paulo"), to_utc_timestamp(df.col("order_purchase_timestamp"), "America/Sao_Paulo")))
        val dfLateDeliveries = df.filter(df.col("delivery_delay(days)") > 10)
        dfLateDeliveries.show
        dfLateDeliveries.write.mode("overwrite").option("header", "true").csv("output/late_deliveries")
    }

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Olist Assignment")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def main(args: Array[String]) = {
        println("Olist Cli: Running ..")
        run(listDelayedDeliveries _)
    }

}