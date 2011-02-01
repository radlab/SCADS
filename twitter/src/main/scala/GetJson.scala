package edu.berkeley.cs
package twitterspam

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.FileInputFormat
import org.apache.hadoop.mapred.InputFormat
import org.apache.hadoop.mapred.InputSplit
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.Reporter
import org.apache.hadoop.util.ReflectionUtils

object GetJson {
  def getJson(path: String): String = {
    val key: LongWritable = new LongWritable
    val value: Text = new Text

    val conf = new JobConf()
    val bufferSize = System.getProperty("spark.buffer.size", "65536")
    conf.set("io.file.buffer.size", bufferSize)
    FileInputFormat.setInputPaths(conf, "s3n://AKIAIGW3SGVZ5RVROFHA:E8Vh0BE4YOR9xJWIHiydWloHEStpLhyQJjTsR05W@ucbcrawler/" + path)
    val fmt = ReflectionUtils.newInstance(classOf[TextInputFormat], conf)
    val inputSplits = fmt.getSplits(conf, 1)
    val reader = fmt.getRecordReader(inputSplits.first, conf, Reporter.NULL)
    reader.next(key, value)
    reader.close

    value.toString
  }
}
