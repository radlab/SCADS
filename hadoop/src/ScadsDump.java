import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class ScadsDump {
	public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			output.collect(key, value);
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ScadsDump.class);
		conf.setJobName("scadsdump");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(ScadsInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(conf, new Path("/scadsdump/"));

		JobClient.runJob(conf);
	}
}
