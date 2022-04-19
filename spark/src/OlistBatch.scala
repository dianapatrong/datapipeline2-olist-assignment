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
            report.coalesce(1).write.mode("Overwrite").option("header", "true").csv(out.concat("report"))

        }

     def test(spark: SparkSession, in: String, out: String) = {
                import java.io.File
                import org.apache.spark.sql.types.TimestampType

                println("--------------------------------------------------------------------------------")
                println("Verify source datasets exist:")
                val orders_dataset = new java.io.File(in.concat("olist_orders_dataset.csv")).exists
                val order_items_dataset = new java.io.File(in.concat("olist_order_items_dataset.csv")).exists
                val products_dataset = new java.io.File(in.concat("olist_products_dataset.csv")).exists
                if ((orders_dataset==true) && (order_items_dataset==true) && (products_dataset==true)){
                    println("1. olist_orders_dataset.csv: exists")
                    println("2. olist_order_items_dataset.csv: exists")
                    println("3. olist_products_dataset.csv: exists")
                }
                else {
                    throw new RuntimeException("ERROR: Dataset files are missing .. ")
                }

                println("--------------------------------------------------------------------------------")
                println("Verify dataset contains important columns:")
                val columns = spark.read.option("header", true).csv(in.concat("olist_orders_dataset.csv")).columns
                if(columns.contains("order_delivered_customer_date") && columns.contains("customer_id") && columns.contains("order_purchase_timestamp")){
                println("Important columns exist: order_delivered_customer_date, customer_id, order_purchase_timestamp")
                }
                else {
                    throw new RuntimeException("ERROR: Some important columns are missing .. ")
                }

                println("--------------------------------------------------------------------------------")
                println("Verify columns order_delivered_customer_date and order_purchase_timestamp can be casted to timestamp data type")
                var df = spark.read.option("header", true).csv(in.concat("olist_orders_dataset.csv")).select("order_delivered_customer_date", "order_purchase_timestamp")
                df = df.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
                df = df.withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))

                val order_delivered_column_dtype = df.schema("order_delivered_customer_date").dataType
                val order_purchase_column_dtype = df.schema("order_purchase_timestamp").dataType

                if (order_delivered_column_dtype.isInstanceOf[TimestampType] && order_purchase_column_dtype.isInstanceOf[TimestampType]){
                    println("order_purchase_timestamp and order_delivered_customer_date columns are correctly casted to TimestampType")
                }
                else{
                    throw new RuntimeException("ERROR: DataTypes were not casted to timestamp .. ")
                }

                println("--------------------------------------------------------------------------------")
                println("Verify there aren't any deliveries in the result which delay is less or equal than 10 days")
                var df1 = spark.read.option("header", true).csv(out.concat("late_deliveries_with_product_information"))
                df1 = df1.filter(col("delivery_delay(days)") <= 10)

                if (df1.count == 0){
                    println("All the deliveries match the criteria of being delayed more than 10 days")
                }
                else {
                    throw new RuntimeException("ERROR: There are some orders that were delayed by 10 or less days  .. ")
                }

                println("--------------------------------------------------------------------------------")
     }
}