package deploylib

/**
 * This is a utility object to give you information about different instance
 * types.
 */
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
  
  /**
   * This method throws an exception if the given string is not one of the
   * EC2 instance types. This method is used internally when
   * DataCenter.runInstances is called. You will likely not need to use it.
   */
  def checkValidType(typeString: String):Boolean = {
    getValue(typeString)
    true
  }
  
  /**
   * This method returns the number of cores a given instance type has.
   */
  def cores(typeString: String): Int = {
    getValue(typeString)  match {
      case Type.m1_small  => 1
      case Type.m1_large  => 2
      case Type.m1_xlarge => 4
      case Type.c1_medium => 2
      case Type.c1_xlarge => 8
    }
  }
  
  /**
   * Returns 32 or 64 depending on the given instance.
   */
  def bits(typeString: String): Int = {
    getValue(typeString) match {
      case Type.m1_small  => 32
      case Type.c1_medium => 32
      case _              => 64
    }
  }
}