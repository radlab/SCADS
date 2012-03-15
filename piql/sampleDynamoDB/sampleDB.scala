import com.amazonaws.auth.PropertiesCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.GetItemResult
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.AmazonServiceException
import scala.collection.JavaConversions._
import java.io.File
import java.text.SimpleDateFormat
import java.util.{Map => JavaMap, Collection => JCollection, Arrays => JArrays}
import java.util.HashMap

object sampleDB {
  var client: AmazonDynamoDBClient = _
//  var dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
	val usersTableName = "Users"
//	val tweetsTableName = "Tweets"
//	val friendsTableName = "Friends"
  
	def main(args: Array[String]) {
	  createClient()
	  
//	  addUserToTable("minh_luong", "123456", 20, "SF")
//	  getUserInfo("minh_luong")
	  
	  println("Finished!")
	}
  
  def createClient() {
//    var credentials = new PropertiesCredentials(getClass().getResourceAsStream("AwsCredentials.properties"))
    var credentials = new PropertiesCredentials(new File("AwsCredentials.properties"))
    client = new AmazonDynamoDBClient(credentials)
  }
  
  def addUserToTable(userID: String, password: String, age: Int, hometown: String) {
  	try {
	    var item: JavaMap[String, AttributeValue] = new HashMap[String, AttributeValue]()
	    item.put("userID", new AttributeValue().withS(userID))
	    item.put("password", new AttributeValue().withS(password))
	    item.put("age", new AttributeValue().withN(age.toString()))
	    item.put("hometown", new AttributeValue().withS(hometown))
	    
	    var itemRequest = new PutItemRequest().withTableName(usersTableName).withItem(item)
	    client.putItem(itemRequest)
	    item.clear() // probably only needed if you want to reuse item later in this method
	    
	    println("Added successfully! :D")
  	} catch {
  	  case ase: AmazonServiceException =>
        sys.error("Failed to put item into table " + usersTableName)
  	  case e: Exception =>
  	}
  }
  
  def getUserInfo(userID: String) {
    var attributesToGet: JCollection[String] = JArrays.asList("userID", "password", "age", "hometown")
    var getItemRequest = new GetItemRequest()
    					.withTableName(usersTableName)
    					.withKey(new Key().withHashKeyElement(new AttributeValue().withS(userID)))
    					.withAttributesToGet(attributesToGet)
    var getItemResult: GetItemResult = client.getItem(getItemRequest)
    
    printItem(getItemResult.getItem())
  }
  
  def printItem(attributeList: JavaMap[String, AttributeValue]) {
    for (item <- attributeList.entrySet()) {
      var attributeName = item.getKey();
      var value = item.getValue();
      println(attributeName
              + "\t"
              + (if (value.getS() == null) "" else "S=[" + value.getS() + "]")
              + (if (value.getN() == null) "" else "N=[" + value.getN() + "]")
              + (if (value.getSS() == null) "" else "SS=[" + value.getSS() + "]")
              + (if (value.getNS() == null) "" else "NS=[" + value.getNS() + "]\n"));
    }
  }
}
