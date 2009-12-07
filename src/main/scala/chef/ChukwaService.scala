package deploylib.chef

import org.json.JSONObject
import org.json.JSONArray
import deploylib._

/*************************
{
    "chukwa": {
        "adaptors": [
            "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Top 15000 /usr/bin/top -b -n 1 -c 0",
            "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Df 60000 /bin/df -x nfs -x none 0",
            "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Sar 1000 /usr/bin/sar -q -r -n ALL 55 0",
            "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Iostat 1000 /usr/bin/iostat -x -k 55 2 0"
        ],
        "collectors": ["r16.millennium.berkeley.edu"]
    },
    "recipes": ["chukwa::default"]
}
*************************/

case class ChukwaService(remoteMachine: RemoteMachine,
                         config: Map[String,Any]) extends ChefService(remoteMachine, config) {
  val cookbookName = "chukwa"
  val recipeName = "default"

  remoteMachine.addService(this)

}
