package edu.berkeley.cs.avro
package runtime

import org.apache.avro.Schema
import org.codehaus.jackson.{JsonFactory, JsonParser, JsonToken}

object DiscoverSchema {
  def main(args: Array[String]): Unit = {
    val parser = JsonObject.factory.createJsonParser(json)
  }

  protected def findSchema(parser: JsonParser, recId: java.lang.Integer): Schema = {
    parser.getCurrentToken match {
      case JsonToken.START_OBJECT => {
	val recName = "Record" + recId
	recId.
	
      case JsonToken.START_ARRAY      if(canBe(schema, Type.ARRAY)) => {
        val array = new GenericData.Array[Any](1, schema)
        while(parser.nextToken != JsonToken.END_ARRAY) {
          array.add(parseValue(parser, schema.getElementType, fieldname))
        }
        array
      }
      case JsonToken.VALUE_STRING       if(canBe(schema, Type.STRING)) => new Utf8(parser.getText)
      case JsonToken.VALUE_NUMBER_INT   if(canBe(schema, Type.LONG)) => parser.getLongValue
      case JsonToken.VALUE_NUMBER_INT   if(canBe(schema, Type.INT)) => parser.getIntValue
      case JsonToken.VALUE_NUMBER_FLOAT if(canBe(schema, Type.DOUBLE)) => parser.getDoubleValue
      case JsonToken.VALUE_TRUE         if(canBe(schema, Type.BOOLEAN)) => true
      case JsonToken.VALUE_FALSE        if(canBe(schema, Type.BOOLEAN)) => false
      case JsonToken.VALUE_NULL         if(canBe(schema, Type.NULL)) => null
      case unexp => {
        val error = "Don't know how to populate field " + fieldname + ". Found: " + parser.getCurrentToken + ", Expected: " + schema
        logger.warning(error)
        throw new RuntimeException(error)
      }
    }
  }

  protected val json = """{
   "id": "367501354973",
   "from": {
      "name": "Bret Taylor",
      "id": "220439"
   },
   "message": "Pigs run from our house in fear. Tonight, I am wrapping the pork tenderloin in bacon and putting pancetta in the corn.",
   "updated_time": "2010-03-06T02:57:48+0000",
   "likes": {
      "data": [
         {
            "id": "29906278",
            "name": "Ross Miller"
         },
         {
            "id": "732777462",
            "name": "Surjit Padham"
         },
         {
            "id": "509411079",
            "name": "Muneer Mirza"
         },
         {
            "id": "1315606682",
            "name": "Sheila Taylor"
         },
         {
            "id": "672745547",
            "name": "Paul Buchheit"
         },
         {
            "id": "836701",
            "name": "Casey Maloney Rosales Muller"
         },
         {
            "id": "1214835",
            "name": "Dan Hsiao"
         },
         {
            "id": "4",
            "name": "Mark Zuckerberg"
         },
         {
            "id": "680980292",
            "name": "April Buchheit"
         },
         {
            "id": "4812961",
            "name": "Ashwin Bharambe"
         },
         {
            "id": "713423888",
            "name": "Andrew Carroll"
         }
      ]
   },
   "comments": {
      "data": [
         {
            "id": "367501354973_12216733",
            "from": {
               "name": "Doug Edwards",
               "id": "628675309"
            },
            "message": "Make sure you don't, as they say, go whole hog...\nhttp://www.youtube.com/watch?v=U4wTFuaV8VQ",
            "created_time": "2010-03-06T03:24:46+0000"
         },
         {
            "id": "367501354973_12249673",
            "from": {
               "name": "Tom Taylor",
               "id": "1249191863"
            },
            "message": "Are you and Karen gonna, as they say, pig out?",
            "created_time": "2010-03-06T21:05:21+0000"
         },
         {
            "id": "367501354973_12249857",
            "from": {
               "name": "Sheila Taylor",
               "id": "1315606682"
            },
            "message": "how did it turn out?  Sounds nummy!\n",
            "created_time": "2010-03-06T21:10:30+0000"
         },
         {
            "id": "367501354973_12250973",
            "from": {
               "name": "Bret Taylor",
               "id": "220439"
            },
            "message": "Mom: turned out well. Sauce was good as well: pan sauce with mustard, balsamic, onion, and maple syrup. Surprisingly good in the end, and not too sweet, which was my concern. ",
            "created_time": "2010-03-06T21:44:53+0000"
         },
         {
            "id": "367501354973_12251276",
            "from": {
               "name": "Sheila Taylor",
               "id": "1315606682"
            },
            "message": "Sounds delicious!  I probably would have skipped the maple syrup, but I am not married to a Canadian :)\n\nSheila Taylor\n(925) 818-7795\nP.O. Box 938\nGlen Ellen, CA 95442\nwww.winecountrytrekking.com",
            "created_time": "2010-03-06T21:55:12+0000"
         },
         {
            "id": "367501354973_12264435",
            "from": {
               "name": "Amelia Ann Arapoff",
               "id": "1580390378"
            },
            "message": "Bacon is always in our refrigerator, in case of need, somehow there is always need.",
            "created_time": "2010-03-07T05:11:52+0000"
         }
      ]
   }
}"""


}
