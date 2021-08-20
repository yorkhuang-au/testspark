package dqr

import java.util.Properties
import java.io.InputStream
import scala.util.{Try,Success,Failure}
import scala.util.control.Exception._

import org.slf4j.Logger
import org.slf4j.LoggerFactory

object Log {
  Try {
    val is: InputStream = ClassLoader.getSystemResourceAsStream("simplelogger.properties")
    val p: Properties = new Properties()
    p.load(is)
    val keys = p.keys
    while (keys.hasMoreElements()) {
        val key = keys.nextElement()
        val value = p.get(key)
        System.setProperty(key.toString, value.toString)
    }
  } match {
    case Failure(e) => throw new Exception(s"Failed to read simplelogger.properties. error: $e")
    case Success(p) => p
  } 

  val logger = LoggerFactory.getLogger("DQR App")  
}
