import com.amazonaws.auth.{PropertiesCredentials, BasicAWSCredentials}
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.GetItemResult
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.AmazonServiceException
import scala.collection.JavaConversions._
import java.util.{Map => JavaMap, Collection => JCollection, Arrays => JArrays}
import java.util.HashMap

object sampleDB {
  var client: AmazonDynamoDBClient = _
	val usersTableName = "Users"
  
	def main(args: Array[String]) {
	  createClient()
	  
//	  addUserToTable("luongm", "123456", 20, "SF")
//	  getUserInfo("luongm")
	  
	  println("Finished!")
	}
  
  def createClient() {
    val accessKey = System.getenv("AWS_ACCESS_KEY_ID")
    val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")

    var credentials = new BasicAWSCredentials(accessKey, secretAccessKey)
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
