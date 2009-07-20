import java.io._
import scala.io.Source

// USAGE: scala parselogs.scala [input_file] [output_file] [aggregation interval in milliseconds] [fraction of requests reported]


val filename = args(0)
val outputfilename = args(1)
val columns = Map("req_type"->1, "starttime"->3, "endtime"->4, "latency"->5)
val datadir = "/tmp/data"
val interval = args(2).toInt  // 10 * 1000
val fractionReported = args(3).toDouble //0.2


val result = MapReduce.mapreduce( List(filename), datadir, (s:String)=>mapByEndtime(s,interval), 
									List(latencyReducer, (s:List[String]) => workloadReducer(s,"throughput",interval,fractionReported)) )
									
val result2 = MapReduce.mapreduce( List(filename), datadir, (s:String)=>mapByStarttime(s,interval), 
									List( (s:List[String]) => workloadReducer(s,"workload",interval,fractionReported)) )

val finalResult = result.combine(result2)
finalResult.saveToFile( outputFilename )
	

def mapByStarttime( line:String, interval:Long ): (String,String) = ( (line.split(",")(columns("starttime")).toLong/interval*interval).toString, line.stripLineEnd)
def mapByEndtime( line:String, interval:Long ): (String,String) = ( (line.split(",")(columns("endtime")).toLong/interval*interval).toString, line.stripLineEnd)

def latencyReducer( lines: List[String] ): Map[String,String] = {
	computeLatencyStats( "all_", lines.map( _.split(",")(columns("latency")).toDouble ) ) ++
	computeLatencyStats( "get_", lines.filter( _.split(",")(columns("req_type"))=="get" ).map( _.split(",")(columns("latency")).toDouble ) ) ++
	computeLatencyStats( "put_", lines.filter( _.split(",")(columns("req_type"))=="put" ).map( _.split(",")(columns("latency")).toDouble ) )
}

def computeLatencyStats( prefix:String, latencies:List[Double] ): Map[String,String] = {
	var m = Map[String,String]()
	m += prefix+"latencyMean" -> computeMean( latencies ).toString
	m += prefix+"latency90Q" -> computeQuantile( latencies, 0.90 ).toString
	m += prefix+"latency99Q" -> computeQuantile( latencies, 0.99 ).toString
	m += prefix+"latency999Q" -> computeQuantile( latencies, 0.999 ).toString
	m
}

def workloadReducer( lines:List[String], outColumnName:String, interval:Long, fractionReported:Double ): Map[String,String] = {
	Map( "all_"+outColumnName -> (lines.length/(interval/1000.0)/fractionReported).toString,
	 	 "get_"+outColumnName -> (lines.filter( _.split(",")(columns("req_type"))=="get" ).length/(interval/1000.0)/fractionReported).toString,
	 	 "put_"+outColumnName -> (lines.filter( _.split(",")(columns("req_type"))=="put" ).length/(interval/1000.0)/fractionReported).toString )
}


object MapReduce {	
	var outfiles = Map[String,BufferedWriter]()

	def mapreduce( files: List[String], datadir: String, mapF: String => (String,String), reduceFs: List[ List[String]=>Map[String,String] ] ): MapReduceResult = {
		(new File(datadir)).mkdirs()
		deleteDirectory(new File(datadir))
		outfiles = Map[String,BufferedWriter]()
		
		mapper( files, datadir, mapF )
		for (file <- outfiles.values) file.close
		val result = reduceFs.map( reducer( datadir, _ ) ).reduceLeft(_.combine(_))
		
		deleteDirectory(new File(datadir))
		result
	}
	
	def mapper( files: List[String], outdir: String, mapF: String => (String,String) ) = {
		for (file <- files) {
			for (line <- Source.fromFile(filename).getLines) {
				val (key, value) = mapF(line)
				val file = if (outfiles.keySet.contains(key)) outfiles(key) else { val f = new BufferedWriter(new FileWriter(outdir+"/"+key)); outfiles+=key->f; f }
				file.write(value+"\n")
			}
		}
	}

	def reducer( dataDir: String, reduceF: List[String]=>Map[String,String] ): MapReduceResult = {
		val dir = new File(dataDir)
		var result = Map[String, Map[String,String]]()
		for (file <- dir.listFiles) {
			val lines = new scala.collection.mutable.ListBuffer[String]
			for (line <- Source.fromFile(file).getLines ) lines += line
			result += file.getName -> reduceF( lines.toList )
		}
		new MapReduceResult(result)
	}
}

class MapReduceResult(
	val data: Map[String,Map[String,String]]
) {
	def combine(that: MapReduceResult): MapReduceResult = {
		val m = scala.collection.mutable.Map[String,Map[String,String]]()
		for (key <- this.data.keySet ++ that.data.keySet) {
			val map1 = if (this.data.keySet.contains(key)) this.data(key) else Map[String,String]()
			val map2 = if (that.data.keySet.contains(key)) that.data(key) else Map[String,String]()
			m += key -> combineMaps(map1,map2)
		}
		new MapReduceResult( Map.empty ++ m	)
	}
	
	def saveToFile( filename: String ) {
		val allkeys = data.keySet.toList.sort(_<_)
		val allcolumns = data.values.map(_.keySet).reduceLeft( Set.empty ++ _ ++ _ ).toList.sort(_<_)

		val f = new BufferedWriter(new FileWriter(filename))
		f.write( "key," + allcolumns.mkString(",") + "\n" )
		for (key <- allkeys) f.write( key + "," + allcolumns.map( data(key)(_) ).mkString(",") + "\n" )
		f.close
	}
}


def deleteDirectory(path: File) {
  if( path.exists() )
    for (file <- path.listFiles())
       if(file.isDirectory()) deleteDirectory(file)
       else file.delete();
}

def combineMaps( map1: Map[String,String], map2: Map[String,String] ): Map[String,String] = {
	val m = scala.collection.mutable.Map[String,String]()
	map1.keys.foreach( k => m+=k->map1(k))
	map2.keys.foreach( k => m+=k->map2(k))
	Map.empty ++ m
}

def computeMean( data: List[Double] ): Double = if (data==Nil) Double.NaN else data.reduceLeft(_+_)/data.length
def computeQuantile( data: List[Double], q: Double): Double = if (data==Nil) Double.NaN else data.sort(_<_)( Math.floor(data.length*q).toInt )