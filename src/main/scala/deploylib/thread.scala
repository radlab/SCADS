
package deploylib

class InstanceThread[T](instance: Instance, fun: (Instance => T)) extends Thread {
  protected var returnValue: T = _
  override def run(): Unit = {
    returnValue = fun(instance)
  }
  def value: T = { this.join(); returnValue }
}