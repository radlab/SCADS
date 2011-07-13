package edu.berkeley.cs.scads.axer

// Simple hashtable optimized for get speed over everything else
// Throws an exception on a colliding put

class AxerIntTable(size:Int) {
  private val vals = Array.fill(size) { -1 }

  def put(key:String, value:Int) {
    put(scala.math.abs(key.##) % size,value)
  }
  
  def put(key:Int,value:Int) {
    if (vals(key) != -1)
      throw new Exception("Collision!")
    vals(key % size) = value
  }

  def get(key:Int):Int = {
    vals(key % size)
  }

  def get(key:String):Int = {
    vals(scala.math.abs(key.##) % size)
  }
}


class AxerRefTable[T <: AnyRef : Manifest](size:Int) {
  private val vals = new Array[T](size)

  def put(key:String, value:T) {
    put(scala.math.abs(key.##) % size,value)
  }
  
  def put(key:Int,value:T) {
    if (vals(key) != null)
      throw new Exception("Collision for: "+key)
    vals(key % size) = value
  }

  def get(key:Int):T = {
    vals(key % size)
  }

  def get(key:String):T = {
    vals(scala.math.abs(key.##) % size)
  }
}
