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

  var adaptors = List(
    "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Top 15000 /usr/bin/top -b -n 1 -c 0",
    "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Df 60000 /bin/df -x nfs -x none 0",
    "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Sar 1000 /usr/bin/sar -q -r -n ALL 55 0",
    "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Iostat 1000 /usr/bin/iostat -x -k 55 2 0")
  
  var collectors = List("r16.millennium.berkeley.edu")

  // TODO: Update adaptors and collectors if necessary.

  remoteMachine.addService(this)

  override def start: Unit = {
    // TODO: Upload JSON Config
    // TODO: Execute command to run recipe
  }

  override def stop: Unit = {
    // TODO: Implement me.
  }

  override def getJSONConfig: String = {
    val chukwaConfig = new JSONObject()
    chukwaConfig.put("recipes", new JSONArray().put(cookbookName + "::" + recipeName))
    val chukwaChukwa = new JSONObject()
    
    val chukwaChukwaAdaptors = new JSONArray()
    for (adaptor <- adaptors) {
      chukwaChukwaAdaptors.put(adaptor)
    }
    chukwaChukwa.put("adaptors", chukwaChukwaAdaptors)
    
    val chukwaChukwaCollectors = new JSONArray()
    for (collector <- collectors) {
      chukwaChukwaCollectors.put(collector)
    }
    chukwaChukwa.put("collectors", chukwaChukwaCollectors)
    
    chukwaConfig.put("chukwa", chukwaChukwa)
    
    return chukwaConfig.toString
  }

}
