package edu.berkeley.cs
package avro
package runtime

import org.apache.avro._
import io._
import specific._

import net.lag.logging.Logger

/**
 * Special vesion of SpecificData allows the user to specify the classloader
 * instead of relying on the classloader used to load SpecificData
 */
class CustomLoaderSpecificData(loader: ClassLoader) extends SpecificData {
  import org.apache.avro.Schema.Type._

  protected val logger = Logger()
  
  private val classCache = new java.util.concurrent.ConcurrentHashMap[String, Class[_]]
  
  val NO_CLASS = new Object(){}.getClass();

  override def getClass(schema: Schema): Class[_] = {
    schema.getType match {
      case FIXED | RECORD | ENUM =>
	val name = schema.getFullName();
        if (name == null) return null
        var c = classCache.get(name)
        if (c == null) {
          try {
            c = loader.loadClass(getClassName(schema))
          } catch {
            case e: ClassNotFoundException =>
	      logger.warning("Failed to locate class %s", name)
	      c = NO_CLASS
          }
          classCache.put(name, c)
        }
        return if(c == NO_CLASS) null else c
      case _ => super.getClass(schema)
    }
  }
}
