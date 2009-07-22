
package deploylib

/**
 * A common class to use to do threaded work on instances.
 *
 * Calling the method value on an instance of this class causes the thread
 * to wait for it to finish and return the what the f returns.
 *
 * @param instance The instance to act on.
 * @param f      A function that will act on the given instance.
 */
class InstanceThread[T](instance: Instance, f: (Instance => T)) extends Thread {
  protected var returnValue: T = _
  override def run(): Unit = {
    returnValue = f(instance)
  }
  
  /**
   * This method causes the thread to join, ie. wait until it is finished.
   * The return is the return of calling f.
   */
  def value: T = { this.join(); returnValue }
}