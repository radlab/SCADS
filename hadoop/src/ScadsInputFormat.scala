import java.io.IOException;
import java.util._;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf._;
import org.apache.hadoop.io._;
import org.apache.hadoop.mapred._;
import org.apache.hadoop.util._;


class ScadsInputFormat extends InputFormat[Text,Text] with JobConfigurable {
	
	def configure(conf: JobConf) {
		
	}
	
	def getRecordReader(split: InputSplit, conf: JobConf, rept: Reporter):RecordReader[Text,Text] = {
		split match {
			case s: ScadsInputSplit => new ScadsRecordReader(s)
			case _ => null
		}
		
	}

	def getSplits(job: JobConf, numSplits: Int): Array[InputSplit] = {
		Array(new ScadsInputSplit());
	}
}