/* dqr.scala */
import scala.util.{Try,Success,Failure}
import java.util.{Timer, TimerTask}
import org.apache.spark.sql.SparkSession
import java.time._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.{SaveMode, DataFrame}
// import com.amazonaws.SDKGlobalConfiguration

object DQR {
  // def runTask(spark: SparkSession) = {
  //   val logFile = "1.txt" 
  //   Console println Instant.now
  //   Console println "-----------------asdfasdf"
  //   val logData = spark.read.textFile(logFile).cache()
  //   val numAs = logData.filter(line => line.contains("a")).count()
  //   println("run 1")
  //   println(s"Lines with a: $numAs")

  //   // delay in milliseconds
  //   val delay = 1000
  //   // remaining args are numbers of partitions to use
  //   val plist = 3 to 8
  //   println("2------------------------")
  //   plist.foreach { np =>
  //     println(s"DELAYED MAP: np= $np")
  //     spark.sparkContext.parallelize(1 to 1000, np).map { x =>
  //       Thread.sleep(delay)
  //       x
  //     }.count
  //   }
  //   println("3------------------------")

  //   // Thread.sleep(5000L)
  //   println("before 2")
  //   val numBs = logData.filter(line => line.contains("b")).count()
  //   print("run 2")
  //   println(s"Lines with a: $numAs, Lines with b: $numBs")
  //   "ok"
  // }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("DQR Application").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val engine = new QueryEngine(spark,
      s"jdbc:impala://myprj1-impala:21050;AuthMech=0",
      new java.util.Properties())

    println(111111)
    val rules = getRules(engine).collect
    println(2222)
    println(rules)
    println(3333)

    rules.foreach{r =>
      println(5555)
      println(r)
      println(666)
    }

    println(4444)

    // val orgDF = engine.read("(select * from dbq1.tbl1) as a")
    // // spark.read.jdbc(jdbcURL, "dbq1.tbl1", connectionProperties)
    // val destDF = orgDF.withColumn("c2", lit(555))

    // engine.write("dbq1.tbl2", destDF, SaveMode.Append)

    // destDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "dbq1.tbl2", connectionProperties)

    // var i = 1
    // while (i<= 5 && !spark.sparkContext.isStopped) {
    //     val t = new Timer()
    //     val task = new TimerTask {
    //         def run() {
    //             println(s"in timer task, i=$i")
    //             // if (i % 3 ==0) {
    //                 spark.stop()
    //             // }
    //             println(s"end timer task i=$i")
    //         }
    //     }

    //     t.schedule(task, 3000L)

    //     Try( runTask(spark)) match {
    //         case Success(ok) => println(s"yay! $ok"); 
    //         case Failure(exception) => println("On no!"); 
    //     }
    //     t.cancel()
    //     i += 1
    // }

    // Console println "11111111111"
    spark.stop()
  }

  /**
    Return rule definitions
  */
  def getRules(engine: QueryEngine) = {
    val sql = """(SELECT m.master_id, r.rule_id, r.param_sql_file, r.summary_sql_file, r.granular_sql_file 
      | FROM dqr.dq_master m JOIN dqr.dq_rule r ON 
      |   m.master_id = r.master_id 
      | WHERE m.master_status=1 AND r.rule_status=1) as rules""".stripMargin
    val rules = engine.read(sql)
    rules
  }
}

class QueryEngine(spark: SparkSession, jdbcUrl: String, properties: java.util.Properties) {
  def read(table: String) = {
    spark.read.jdbc(jdbcUrl, table, properties)
  }
  def write(table: String, df: DataFrame, mode: SaveMode) = {
    df.write.mode(mode).jdbc(jdbcUrl, table, properties)
  }
}

