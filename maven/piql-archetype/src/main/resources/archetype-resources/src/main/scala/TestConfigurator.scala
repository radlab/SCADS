package piql

import edu.berkeley.cs.scads.piql.{ Entity, Environment, QueryExecutor }

class TestConfigurator {
  def configureTestCluster = Configurator.configure()
}

class QueryHelper extends QueryExecutor {
  def all(entityName: String, limit: Int, offset: Int, backwards: Boolean)(implicit env: Environment): Seq[Entity[_,_]] = { 
    val ns = "ent_" + entityName
    env.namespaces.get(ns) match {
      case Some(namespace) =>  
        val kvSeq = namespace.getRange(null, null, limit, offset, backwards) // fetch everything, limit to limit, ascending 
        val clazzName = "piql." + entityName
        val entClass = Class.forName(clazzName)
        if (entClass eq null) 
          throw new IllegalArgumentException("Could not find class for name: " + clazzName)
        materialize(entClass.asInstanceOf[Class[Entity[_,_]]], kvSeq)
      case None =>
        throw new IllegalArgumentException("Bad entity name: " + entityName)
    }   
  }
}
