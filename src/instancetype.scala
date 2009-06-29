package deploylib

object InstanceType {
  private object Type extends Enumeration {
    val m1_small  = Value("m1.small")
    val m1_large  = Value("m1.large")
    val m1_xlarge = Value("m1.xlarge")
    val c1_medium = Value("c1.medium")
    val c1_xlarge = Value("c1.xlarge")
  }
  
  private def getValue(typeString: String): Type.Value = {
    try {
      Type.valueOf(typeString).get
    } catch {
      case ex: NoSuchElementException => throw new NoSuchElementException(
        "Not a valid instance type: " + typeString)
    }
  }
  
  def cores(typeString: String): Int = {
    getValue(typeString)  match {
      case Type.m1_small  => 1
      case Type.m1_large  => 2
      case Type.m1_xlarge => 4
      case Type.c1_medium => 2
      case Type.c1_xlarge => 8
    }
  }
  
  def bits(typeString: String): String = {
    getValue(typeString) match {
      case Type.m1_small  => "32-bit"
      case Type.c1_medium => "32-bit"
      case _              => "64-bit"
    }
  }
}