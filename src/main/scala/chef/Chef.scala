package deploylib.chef

import deploylib._

import java.io.File

/**
 * Contains helper methods for services deployed using Chef.
 */
object Chef {
  /**
   * Path to the Chef repository (should be *.tar.gz).
   */
  var repoPath: String = null
}

/**
 * A framework for managing services that are deployed using Chef.
 */
abstract class ChefService(remoteMachine: RemoteMachine,
                           config: Map[String,Any]) extends Service(remoteMachine) {

  /**
   * The name of the chef cookbook containing the recipe.
   */
  val cookbookName: String

  /**
   * The name of the chef recipe used to deploy this service.
   */
  val recipeName: String

  /**
   * Returns the JSON config as a String needed to deploy this service
   * with Chef Solo.
   */
  def getJSONConfig: String

  /**
   * Upload chef repo to /tmp
   */
  override def start: Unit = {
    remoteMachine.upload(new File(Chef.repoPath), new File("/tmp"))
  }


  /**
   * Update the JSON config object and add to dependencies.
   */
  override def addDependency(service: Service): Unit = {
    // TODO: Throw an exception for unhandled dependency.
  }

}
