import zio._
import org.apache.spark.sql.SparkSession

object OlistCli extends ZIOApp {

    import org.apache.log4j.Logger

    type Environment = ZEnv

    val tag = Tag[Environment]

    override def layer: ZLayer[Has[ZIOAppArgs],Any,Environment] = ZLayer.wire[Environment](ZEnv.live)

    def run(command: Array[String]) = command match {
        case Array("batch", in, out) => OlistBatch.run((spark: SparkSession) => OlistBatch.listDelayedDeliveries(spark, in, out))
        case Array("report", in, out) => OlistBatch.run((spark: SparkSession) => OlistBatch.report(spark, in, out))
        case Array("test", in, out) => OlistBatch.run((spark: SparkSession) => OlistBatch.test(spark, in, out))
        case _ => println(s"command '$command' not recognized (batch|index)")
    }
    override def run: ZIO[Environment with ZEnv with Has[ZIOAppArgs],Any,Any] = for {
     args <- getArgs if args.length > 0
     _ <- ZIO.attempt(run(args.toArray))
     _ <- Console.printLine(s"finished")
    } yield ()

}
