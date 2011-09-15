package org.chris.features

import scala.collection.mutable._

trait Vectorizable { 
  def get[T](f: java.lang.reflect.Field, a: AnyRef): T = {
    f.setAccessible(true)
    f.get(a).asInstanceOf[T]
  }

  def isSimple(f: java.lang.reflect.Field): Boolean = {
    val typ = f.getType()
    isSimple(typ)
  }

  def isSimple(c: java.lang.Class[_]): Boolean = {
    if(c == classOf[String] || c == classOf[Boolean] ||
       c == classOf[Int] || c == classOf[Long] || 
       c == classOf[Long] || c == classOf[Option[Any]])
      true
    else
      false
  }

  def mergeBoolean(bools : List[Option[Boolean]]) : String = {
      var all = true
      var some = false
      var set = false

      bools.foreach(m =>
          m match {
              case None => 0
              case Some(x) => 
                    all = all && x
                    some = some || x
                    set = true
          })

      if (set == false) {
          return "null"
      }
      else if (all == true) {
          return "all"
      }
      else if (some == true) {
          return  "some"
      }
      else {
          return "none"
      }
  }

  def makeName(vari: Any, name: String, name2: String = "") : (String, Any) = vari match { 
    case s:  String => ("bit_%s_%s_%s".format(name, name2, vari), 1)
    case b:  Boolean => ("bit_%s_%s".format(name, name2), if(b) 1 else 0)
    case i:  Int => ("real_%s_%s".format(name, name2), i)
    case l:  Long => ("real_%s_%s".format(name, name2), l)
    case o:  Option[Any] => o match { 
      case Some(s) => makeName(s, name, name2) 
      case None => ("", None)
    }
    case _ => ("", None)
  }

  def vectorizeFields(infields: List[String], name1: String) : HashMap[String,Any] = {
    val map = new HashMap[String, Any]
    val fields = getClass.getDeclaredFields.toList.filter(f => infields.contains(f.getName()))
    val vectorized = fields.map(f => makeName(get(f, this), name1, f.getName()))
    for(m <- vectorized) {
      map += m._1 -> m._2
    }
    map
  }

  def vectorizeSimple(name1: String, exclude:List[String]=List()) : HashMap[String,Any] = {
    val map = new HashMap[String, Any]
    val fields = getClass.getDeclaredFields.toList.filter(!_.isSynthetic).filterNot(s => ("url"::exclude).contains(s.getName()))

    // this is kinda complicated, but it will auto convert lists!
    val listfields = fields.filter(f => f.getType() == classOf[List[Any]])
    for(l <- listfields) { 
      val thelist : List[Any] = get(l, this)
      if(thelist.length > 0 && isSimple(thelist(0).asInstanceOf[AnyRef].getClass)) {
        val vectorized = thelist.map(m => makeName(m, name1, l.getName()))
        for(m <- vectorized) {
          map += m._1 -> m._2
        }
      }
    }

    val simplefields = fields.filter(s => isSimple(s))
    val vectorized = simplefields.map(m => makeName(get(m, this), name1, m.getName()))
    for(m <- vectorized) {
      map += m._1 -> m._2
    }

    val urlf = fields.filter(f => f.getName() == "url")
    if(urlf.length > 0) {
      //println("Auto-formatting URL in " + name1)
      URLTool.parseURL(get(urlf(0), this)) match {
        case Some(s) => map ++= s.vectorize(name1 + "_url")
        case None =>
      }
    }

    map
  }

  def vectorize(name1: String) : HashMap[String,Any] = vectorizeSimple(name1)
}


