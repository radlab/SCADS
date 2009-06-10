import java.io.IOException;
import java.util._;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;


case class ScadsRecordReader(split: ScadsInputSplit) extends org.apache.hadoop.mapred.RecordReader[Text,Text] {
	def next(key: Text, value:Text): Boolean = {
		val k = split.readNext()
		if(k.length == 0)
			return false

		key.set(k)
		value.set(split.readNext())
		true
	}

	def close() {}
	def createKey():Text = new Text()
	def createValue(): Text = new Text()
	def getPos(): Long = 0
	def getProgress(): Float = 0
}
