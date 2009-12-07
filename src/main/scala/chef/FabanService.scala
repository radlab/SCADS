package deploylib.chef

import deploylib._

/*************************
{
    "recipes": ["cloudstone::faban"],
    "faban": {
        "hosts": {
            "driver":    "[Internal IP of Driver]",
            "webserver": "[Internal IP of Web Server]",
            "database":  "[Internal IP of DB]",
            "storage":   "",
            "cache":     ""
        },
        "database": {
            "adapter": "mysql",
            "port": 3306
        },
        "debug": false
    }
}

*************************/

case class FabanService(remoteMachine: RemoteMachine,
                        config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "cloudstone"
  val recipeName = "rails"

  remoteMachine.addService(this)

}
