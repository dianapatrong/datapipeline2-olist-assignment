import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OlistCli {

    def listDelayedDeliveries(spark: SparkSession) = {
        val dfOrders = spark.read.option("header", true).csv("data/archive/olist_orders_dataset.csv")
        var dfLateDeliveries = dfOrders.withColumn("delivery_delay(days)", datediff(to_utc_timestamp(dfOrders.col("order_delivered_customer_date"), "America/Sao_Paulo"), to_utc_timestamp(dfOrders.col("order_purchase_timestamp"), "America/Sao_Paulo")))
        dfLateDeliveries = dfLateDeliveries.select("order_id", "customer_id", "order_delivered_customer_date", "order_purchase_timestamp", "delivery_delay(days)").filter(dfLateDeliveries.col("delivery_delay(days)") > 10)

        val dfOrderItems = spark.read.option("header", true).csv("data/archive/olist_order_items_dataset.csv").select("order_id", "product_id")
        val dfItems = spark.read.option("header", true).csv("data/archive/olist_products_dataset.csv").select("product_id", "product_category_name")

        var dfLateDeliveriesWithProductInfo = dfLateDeliveries.join(dfOrderItems, Seq("order_id"), "inner").join(dfItems, Seq("product_id"), "inner")
        dfLateDeliveriesWithProductInfo.show
        dfLateDeliveriesWithProductInfo.repartition(1).write.mode("overwrite").option("header", "true").csv("output/late_deliveries")
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