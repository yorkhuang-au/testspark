package dqr

// import scala.io.Source
import java.util.Properties
import java.io.InputStream
import scala.util.{Try,Success,Failure}
import scala.util.control.Exception._
import java.util.UUID

object Config {
  val config = Try {
    val is: InputStream = ClassLoader.getSystemResourceAsStream("appconf.properties")
    val properties: Properties = new Properties()
    properties.load(is)
    properties
  } match {
    case Failure(e) => throw new Exception(s"Failed to read appconf.properties. error: $e")
    case Success(p) => p
  }
  def getProperty(key: String): String = config.getProperty(key).trim
  def getID(): String = UUID.randomUUID().toString
  lazy val batchID: Long = System.currentTimeMillis()
  lazy val sourceApplication: String = config.getProperty("source.application")
  lazy val applicationJobName: String = config.getProperty("application.job.name")
  
  val GRANULAR_RULE_LEVEL = 1
  val SUMMARY_RULE_LEVEL = 2
  val BOTH_RULE_LEVEL = 0

  val ALL_GROUP = 0

  val RUN_START_STATUS = 0
  val RUN_SUCCESS_STATUS = 1
  val RUN_FAILURE_STATUS = 2
  val RUN_PARAMETER_FAILURE = 3

  def getAppParmeter(args: Array[String]): AppParameter = {
    val argMap = args.flatMap(_.split("=")).sliding(2, 2).toList.map(a=>(a(0).replaceAll("^--", "") -> a(1))).toMap

    val level =  (if(argMap.contains("level")) argMap("level") 
      else Try(Config.getProperty("dq.rule.level")).toOption.getOrElse(Config.BOTH_RULE_LEVEL.toString))
      .trim.toInt match {
        case Config.GRANULAR_RULE_LEVEL => Config.GRANULAR_RULE_LEVEL
        case Config.SUMMARY_RULE_LEVEL => Config.SUMMARY_RULE_LEVEL
        case _ => Config.BOTH_RULE_LEVEL
      }

    
    
    // Try((if(argMap.contains("level")) argMap("level") else Config.getProperty("dq.rule.level")).trim.toInt)
    //   .toOption.getOrElse(Config.BOTH_RULE_LEVEL) match {
    //   case Config.GRANULAR_RULE_LEVEL => Config.GRANULAR_RULE_LEVEL
    //   case Config.SUMMARY_RULE_LEVEL => Config.SUMMARY_RULE_LEVEL
    //   case _ => Config.BOTH_RULE_LEVEL
    // }

    val datekeyExtra = (if(argMap.contains("datekey_extra")) argMap("datekey_extra")
      else Try(Config.getProperty("dq.datekey.extra")).toOption.getOrElse("0")).trim.toInt
    
    //  Try((if(argMap.contains("datekey_extra")) argMap("datekey_extra") else Config.getProperty("dq.datekey.extra"))
    //   .trim.toInt).toOption.getOrElse(0)
    
    val groupNo = (if(argMap.contains("group")) argMap("group") 
      else Try(Config.getProperty("dq.run.group")).toOption.getOrElse(Config.ALL_GROUP.toString)).trim.toInt
    
    // Try((if(argMap.contains("group")) argMap("group") else Config.getProperty("dq.run.group")).trim.toInt).toOption.getOrElse(Config.ALL_GROUP)

    val ruleIDs = if(!argMap.contains("rule_id")) List[Int]()
      else argMap("rule_id").split("[,;:]").map(_.trim.toInt).toSet.toList

    AppParameter(groupNo, datekeyExtra, level, ruleIDs)
  }
}


case class AppParameter(groupNo: Int, datekeyExtra: Int, ruleLevel: Int, ruleIDs: List[Int])