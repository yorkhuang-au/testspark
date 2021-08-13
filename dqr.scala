// /* dqr.scala */
// import scala.util.{Try,Success,Failure}
// import java.util.{Timer, TimerTask}
// import org.apache.spark.sql.SparkSession
// import java.time._
// import org.apache.spark.rdd.RDD
// import org.apache.spark.sql.functions.{lit}
// import org.apache.spark.sql.{SaveMode, DataFrame}
// // import com.amazonaws.SDKGlobalConfiguration

// object DQR {
//   // def runTask(spark: SparkSession) = {
//   //   val logFile = "1.txt" 
//   //   Console println Instant.now
//   //   Console println "-----------------asdfasdf"
//   //   val logData = spark.read.textFile(logFile).cache()
//   //   val numAs = logData.filter(line => line.contains("a")).count()
//   //   println("run 1")
//   //   println(s"Lines with a: $numAs")

//   //   // delay in milliseconds
//   //   val delay = 1000
//   //   // remaining args are numbers of partitions to use
//   //   val plist = 3 to 8
//   //   println("2------------------------")
//   //   plist.foreach { np =>
//   //     println(s"DELAYED MAP: np= $np")
//   //     spark.sparkContext.parallelize(1 to 1000, np).map { x =>
//   //       Thread.sleep(delay)
//   //       x
//   //     }.count
//   //   }
//   //   println("3------------------------")

//   //   // Thread.sleep(5000L)
//   //   println("before 2")
//   //   val numBs = logData.filter(line => line.contains("b")).count()
//   //   print("run 2")
//   //   println(s"Lines with a: $numAs, Lines with b: $numBs")
//   //   "ok"
//   // }

//   def main(args: Array[String]) {
//     val spark = SparkSession.builder.appName("DQR Application").getOrCreate()
//     spark.sparkContext.setLogLevel("ERROR")

//     val engine = new QueryEngine(spark,
//       s"jdbc:impala://myprj1-impala:21050;AuthMech=0",
//       new java.util.Properties())

//     println(111111)
//     val rules = getRules(engine).collect
//     println(2222)
//     println(rules)
//     println(3333)

//     rules.foreach{r =>
//       println(5555)
//       println(r)
//       println(666)
//     }

//     println(4444)

//     // val orgDF = engine.read("(select * from dbq1.tbl1) as a")
//     // // spark.read.jdbc(jdbcURL, "dbq1.tbl1", connectionProperties)
//     // val destDF = orgDF.withColumn("c2", lit(555))

//     // engine.write("dbq1.tbl2", destDF, SaveMode.Append)

//     // destDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "dbq1.tbl2", connectionProperties)

//     // var i = 1
//     // while (i<= 5 && !spark.sparkContext.isStopped) {
//     //     val t = new Timer()
//     //     val task = new TimerTask {
//     //         def run() {
//     //             println(s"in timer task, i=$i")
//     //             // if (i % 3 ==0) {
//     //                 spark.stop()
//     //             // }
//     //             println(s"end timer task i=$i")
//     //         }
//     //     }

//     //     t.schedule(task, 3000L)

//     //     Try( runTask(spark)) match {
//     //         case Success(ok) => println(s"yay! $ok"); 
//     //         case Failure(exception) => println("On no!"); 
//     //     }
//     //     t.cancel()
//     //     i += 1
//     // }

//     // Console println "11111111111"
//     spark.stop()
//   }

//   /**
//     Return rule definitions
//   */
//   def getRules(engine: QueryEngine) = {
//     val sql = """(SELECT m.master_id, r.rule_id, r.param_sql_file, r.summary_sql_file, r.granular_sql_file 
//       | FROM dqr.dq_master m JOIN dqr.dq_rule r ON 
//       |   m.master_id = r.master_id 
//       | WHERE m.master_status=1 AND r.rule_status=1) as rules""".stripMargin
//     val rules = engine.read(sql)
//     rules
//   }
// }

// class QueryEngine(spark: SparkSession, jdbcUrl: String, properties: java.util.Properties) {
//   def read(table: String) = {
//     spark.read.jdbc(jdbcUrl, table, properties)
//   }
//   def write(table: String, df: DataFrame, mode: SaveMode) = {
//     df.write.mode(mode).jdbc(jdbcUrl, table, properties)
//   }
// }




// // /* dqr.scala */
// // import scala.util.{Try,Success,Failure}
// // import java.util.{Timer, TimerTask}
// // import org.apache.spark.sql.SparkSession
// // import java.time._
// // import org.apache.spark.rdd.RDD
// // import org.apache.spark.sql.functions.{lit}
// // import org.apache.spark.sql.{SaveMode, DataFrame}
// // // import com.amazonaws.SDKGlobalConfiguration

// // object DQR {
// //   // def runTask(spark: SparkSession) = {
// //   //   val logFile = "1.txt" 
// //   //   Console println Instant.now
// //   //   Console println "-----------------asdfasdf"
// //   //   val logData = spark.read.textFile(logFile).cache()
// //   //   val numAs = logData.filter(line => line.contains("a")).count()
// //   //   println("run 1")
// //   //   println(s"Lines with a: $numAs")

// //   //   // delay in milliseconds
// //   //   val delay = 1000
// //   //   // remaining args are numbers of partitions to use
// //   //   val plist = 3 to 8
// //   //   println("2------------------------")
// //   //   plist.foreach { np =>
// //   //     println(s"DELAYED MAP: np= $np")
// //   //     spark.sparkContext.parallelize(1 to 1000, np).map { x =>
// //   //       Thread.sleep(delay)
// //   //       x
// //   //     }.count
// //   //   }
// //   //   println("3------------------------")

// //   //   // Thread.sleep(5000L)
// //   //   println("before 2")
// //   //   val numBs = logData.filter(line => line.contains("b")).count()
// //   //   print("run 2")
// //   //   println(s"Lines with a: $numAs, Lines with b: $numBs")
// //   //   "ok"
// //   // }

// //   def main(args: Array[String]) {
// //     val spark = SparkSession.builder.appName("DQR Application").getOrCreate()
// //     spark.sparkContext.setLogLevel("ERROR")

// //     val connectionProperties = new java.util.Properties()
// //     // connectionProperties.setProperty("driver","com.cloudera.impala.jdbc41.Driver")
// //     val engine = new QueryEngine(spark,
// //       s"jdbc:impala://myprj1-impala:21050;AuthMech=0",
// //       connectionProperties)

// //     val rules = getRules(engine).collect

// //     rules.foreach{r =>
// //       println(r)

// //       val dqTask = new DQTask(spark, engine, r.getAs[Int]("rule_id"), r.getAs[String]("param_sql_file"), 
// //         r.getAs[String]("summary_sql_file"), r.getAs[String]("granular_sql_file"))

// //       val sqlp = dqTask.getSqlParam()
// //       println(sqlp)
      
// //     }

// //     // val orgDF = engine.read("(select * from dbq1.tbl1) as a")
// //     // // spark.read.jdbc(jdbcURL, "dbq1.tbl1", connectionProperties)
// //     // val destDF = orgDF.withColumn("c2", lit(555))

// //     // engine.write("dbq1.tbl2", destDF, SaveMode.Append)

// //     // destDF.write.mode(SaveMode.Append).jdbc(jdbcURL, "dbq1.tbl2", connectionProperties)

// //     // var i = 1
// //     // while (i<= 5 && !spark.sparkContext.isStopped) {
// //     //     val t = new Timer()
// //     //     val task = new TimerTask {
// //     //         def run() {
// //     //             println(s"in timer task, i=$i")
// //     //             // if (i % 3 ==0) {
// //     //                 spark.stop()
// //     //             // }
// //     //             println(s"end timer task i=$i")
// //     //         }
// //     //     }

// //     //     t.schedule(task, 3000L)

// //     //     Try( runTask(spark)) match {
// //     //         case Success(ok) => println(s"yay! $ok"); 
// //     //         case Failure(exception) => println("On no!"); 
// //     //     }
// //     //     t.cancel()
// //     //     i += 1
// //     // }

// //     spark.stop()
// //   }

// //   /**
// //     Return rule definitions
// //   */
// //   def getRules(engine: QueryEngine) = {
// //     val sql = """(SELECT m.master_id, r.rule_id, r.param_sql_file, r.summary_sql_file, r.granular_sql_file 
// //       | FROM dqr.dq_master m JOIN dqr.dq_rule r ON 
// //       |   m.master_id = r.master_id 
// //       | WHERE m.master_status=1 AND r.rule_status=1) as rules""".stripMargin
// //     val rules = engine.read(sql)
// //     rules
// //   }
// // }

// // class QueryEngine(spark: SparkSession, jdbcUrl: String, properties: java.util.Properties) {
// //   def read(table: String) = {
// //     println(table)
// //     val df = spark.read.jdbc(jdbcUrl, table, properties)
// //     df.show()
// //     df
// //   }
// //   def write(table: String, df: DataFrame, mode: SaveMode) = {
// //     df.write.mode(mode).jdbc(jdbcUrl, table, properties)
// //   }
// // }

// // class DQTask(spark: SparkSession, engine: QueryEngine, dq_id: Int, sqlfp: String, sqlfs: String, sqlfg: String) {
// //   def getSqlParam():Map[String, AnyVal] = {
// //     val sqlp = readSQLFile(sqlfp)
// //     if(sqlp.isEmpty)
// //       return Map[String, AnyVal]()
    
// //     val df = engine.read(s"($sqlp) para")
// //     df.show
// //     val row = df.first
// //     println(row)
// //     val result = row.getValuesMap[AnyVal](row.schema.fieldNames)
// //     println(result)
// //     result
// //   }

// //   def readSQLFile(filename: String):String = {
// //     if(filename == null || filename.trim.isEmpty)
// //       return ""

// //     val ar = spark.sparkContext.wholeTextFiles(filename).collect
// //     val s = ar(0)._2.trim
// //     val ss = 
// //     if(s.endsWith(";"))
// //       s.dropRight(1).trim
// //     else
// //       s
// //     println(s"ss=$ss")
// //     ss
// //   }
// // }



import java.sql.DriverManager
import java.sql.Connection

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import com.amazonaws.client.builder.AwsClientBuilder
import scala.collection.JavaConversions._
import java.io.File
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileOutputStream

import java.sql.{Connection, ResultSet, SQLException, Statement}

/* dqr.scala */
import scala.util.{Try,Success,Failure}
// import java.util.{Timer, TimerTask}
// import org.apache.spark.sql.SparkSession
// import java.time._
// import org.apache.spark.rdd.RDD
// import org.apache.spark.sql.functions.{lit}
// import org.apache.spark.sql.{SaveMode, DataFrame}
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

    val engine = new QueryEngine("jdbc:impala://myprj1-impala:21050;AuthMech=0")

    val rules = getRules(engine)

    try {
      while ( rules.next() ) {
        val masterID = rules.getString("master_id")
        println(s"master_id=$masterID" )
      }
    } catch {
      case e:Throwable => e.printStackTrace
    }

    // rules.foreach{r =>
    //   println(r)

    //   val dqTask = new DQTask(spark, engine, r.getAs[Int]("rule_id"), r.getAs[String]("param_sql_file"), 
    //     r.getAs[String]("summary_sql_file"), r.getAs[String]("granular_sql_file"))

    //   val sqlp = dqTask.getSqlParam()
    //   println(sqlp)
      
    // }
    engine.close()
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

class QueryEngine(url: String) {
  val driver = "com.cloudera.impala.jdbc41.Driver"
  // val url = "jdbc:impala://myprj1-impala:21050;AuthMech=0"

  Class.forName(driver)
  val connection = DriverManager.getConnection(url)
  val statement = connection.createStatement()

  def close() {
    connection.close()
  }

  def read(query: String) = {
    Try {
      statement.executeQuery(query)
    } 
  }

  // def write(table: String, df: DataFrame, mode: SaveMode) = {
  //   df.write.mode(mode).jdbc(jdbcUrl, table, properties)
  // }
}

// class DQTask(spark: SparkSession, engine: QueryEngine, dq_id: Int, sqlfp: String, sqlfs: String, sqlfg: String) {
//   def getSqlParam():Map[String, AnyVal] = {
//     val sqlp = readSQLFile(sqlfp)
//     if(sqlp.isEmpty)
//       return Map[String, AnyVal]()
    
//     val df = engine.read(s"($sqlp) para")
//     df.show
//     val row = df.first
//     println(row)
//     val result = row.getValuesMap[AnyVal](row.schema.fieldNames)
//     println(result)
//     result
//   }

//   def readSQLFile(filename: String):String = {
//     if(filename == null || filename.trim.isEmpty)
//       return ""

//     val ar = spark.sparkContext.wholeTextFiles(filename).collect
//     val s = ar(0)._2.trim
//     val ss = 
//     if(s.endsWith(";"))
//       s.dropRight(1).trim
//     else
//       s
//     println(s"ss=$ss")
//     ss
//   }
// }

///////////////

// object DQR {

//   def main(args: Array[String]) {
//     val driver = "com.cloudera.impala.jdbc41.Driver"
//     val url = "jdbc:impala://myprj1-impala:21050;AuthMech=0"

//     var connection:Connection = null

//     try {
//       Class.forName(driver)
//       connection = DriverManager.getConnection(url)

//       val statement = connection.createStatement()
//       val resultSet = statement.executeQuery("SELECT from_timestamp(now(), 'yyyyMMdd') as master_id")
//       while ( resultSet.next() ) {
//         val host = resultSet.getString("master_id")
//         println(s"host=$host" )
//       }
//     } catch {
//       case e:Throwable => e.printStackTrace
//     }
//     connection.close()

//     val s3Client = AmazonS3ClientBuilder.standard()
//       .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
//         "http://myprj1-localstack:4566", Regions.AP_SOUTHEAST_2.name() ))
//       .withCredentials(new DefaultAWSCredentialsProviderChain) 
//       .withPathStyleAccessEnabled(true)
//       .build()
      
//     val bucketName ="dqr-rules"
//     val bucketpreFix= ""
//     var objectListing: ObjectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName).withPrefix(bucketpreFix))
//     var objectSummaries: List[S3ObjectSummary] = objectListing.getObjectSummaries.toList
//     val listOFKeys =  objectSummaries.map(o => o.getKey)
//     val fileSize = objectSummaries.map(o => o.getSize).sum

//     println(s"Reading files : ${listOFKeys.length} Size ${fileSize/(1024.0*1024.0)} MB")
//     listOFKeys.foreach(println)

//     val data = listOFKeys.map{
//       key =>
//         val s3object: S3Object = s3Client.getObject(new GetObjectRequest(bucketName, key))
//         val objectData = s3object.getObjectContent
//         val bytes = scala.io.Source.fromInputStream(objectData).mkString.getBytes
//         objectData.close()
//         (bytes.map(_.toChar)).mkString 
//     }
//     println(data)
//   }
// }

