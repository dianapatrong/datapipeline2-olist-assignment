import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OlistBatch {

    def listDelayedDeliveries(spark: SparkSession, in: String, out: String) = {
        val dfOrders = spark.read.option("header", true).csv(in.concat("olist_orders_dataset.csv"))
        var dfLateDeliveries = dfOrders.withColumn("delivery_delay(days)", datediff(to_utc_timestamp(dfOrders.col("order_delivered_customer_date"), "America/Sao_Paulo"), to_utc_timestamp(dfOrders.col("order_purchase_timestamp"), "America/Sao_Paulo")))
        dfLateDeliveries = dfLateDeliveries.select("order_id", "customer_id", "order_delivered_customer_date", "order_purchase_timestamp", "delivery_delay(days)").filter(dfLateDeliveries.col("delivery_delay(days)") > 10)
        println("Writing information for customers with late deliveries ..")
        dfLateDeliveries.select("customer_id").distinct().repartition(1).write.mode("overwrite").option("header", "true").csv(out.concat("customers_with_late_deliveries"))

        val dfOrderItems = spark.read.option("header", true).csv(in.concat("olist_order_items_dataset.csv")).select("order_id", "product_id")
        val dfItems = spark.read.option("header", true).csv(in.concat("olist_products_dataset.csv")).select("product_id", "product_category_name")

        var dfLateDeliveriesWithProductInfo = dfLateDeliveries.join(dfOrderItems, Seq("order_id"), "inner").join(dfItems, Seq("product_id"), "inner")
        dfLateDeliveriesWithProductInfo.show
        println("Writing information for products that were delivered late ..")
        dfLateDeliveriesWithProductInfo.repartition(1).write.mode("Overwrite").option("header", "true").csv(out.concat("late_deliveries_with_product_information"))
    }

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Olist Assignment")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def report(spark: SparkSession, in: String, out: String) = {
            import spark.implicits._
            var df = spark.read.option("header", true).csv(in.concat("olist_orders_dataset.csv"))

            df = df.withColumn("date", col("order_purchase_timestamp").cast("date"))
            df = df.withColumn("delivery_delay", datediff(to_utc_timestamp(df.col("order_delivered_customer_date"), "America/Sao_Paulo"), to_utc_timestamp(df.col("order_purchase_timestamp"), "America/Sao_Paulo")))

            df.createOrReplaceTempView("logs")
            val dates = spark.sql("""
            with dates as (
                select date, count(*) as cnt
                from logs
                where delivery_delay > 10
                group by date
                order by cnt desc)

            select * from dates
            """).createTempView("dates")

            val report = spark.sql("""
                    select *
                    from dates
                    """)
            report.coalesce(1).write.mode("Overwrite").option("header", "true").csv(out)

        }

}