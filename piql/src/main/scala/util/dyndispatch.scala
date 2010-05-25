package edu.berkeley.cs.scads.piql

object DynamicDispatch {
  implicit def toDynDispObj(obj: AnyRef): DynamicDispatchObject = new DynamicDispatchObject(obj)
  implicit def toDynDispClass[T](cls: Class[T]): DynamicDispatchClass[T] = new DynamicDispatchClass(cls)
}

class DynamicDispatchObject(obj: AnyRef) {
  val cls = obj.getClass

  def setField(fieldName: String, value: Object): Unit = {
    val mutator = cls.getMethods.filter(_.getName.equals(fieldName + "_$eq")).head
    mutator.invoke(obj, value)
  }

  def getField(fieldName: String): Any = {
    val accesor = cls.getMethod(fieldName)
    accesor.invoke(obj)
  }

  def call(methodName: String, args: List[AnyRef]): AnyRef = {
    val method = cls.getMethod(methodName, args.map(_.getClass):_*)
    method.invoke(obj, args)
  }
}

class DynamicDispatchClass[ClassType](cls: Class[ClassType]) {
  def newInstance2(args: AnyRef*): ClassType = {
    val constructor = cls.getConstructor(args.map(_.getClass):_*)
    constructor.newInstance(args:_*).asInstanceOf[ClassType]
  }

  def callStatic(methodName: String, args: List[AnyRef]): AnyRef = {
    val method = cls.getMethod(methodName, args.map(_.getClass):_*)
    method.invoke(cls, args:_*)
  }
}
