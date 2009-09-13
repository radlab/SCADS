package performance

import scala.collection.jcl.Conversions._

import edu.berkeley.cs.scads.client._
import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.nodes._
import edu.berkeley.cs.scads.keys._
import edu.berkeley.cs.scads.placement._

import java.io._
import java.net._
import scala.io._
import scala.collection.mutable.ListBuffer


//////
// workload description
//
@serializable
class WorkloadIntervalDescription(
	val numberOfActiveUsers: Int,
	val duration: Int,
	val requestGenerator: SCADSRequestGenerator)


@serializable
class WorkloadDescription(
	val thinkTimeMean: Long, 
	val workload: List[WorkloadIntervalDescription]
	) 
{
	def getMaxNUsers(): Int = workload.map(_.numberOfActiveUsers).reduceLeft(Math.max(_,_))
	
	def serialize(file: String) = {
        val out = new ObjectOutputStream( new FileOutputStream(file) )
        out.writeObject(this);
        out.close();
	}
}

object WorkloadDescription {
	var rnd = new java.util.Random
	
	def setRndGenerator(r:java.util.Random) { rnd = r }
	
	def create(workloadProfile:WorkloadProfile, durations:List[Int], requestGenerators:List[SCADSRequestGenerator], thinkTimeMean:Long): WorkloadDescription = {
		assert(workloadProfile.profile.length==durations.length && workloadProfile.profile.length==requestGenerators.length, 
			"workloadProfile, durations, and requestGenerators need to have the same length")
		val intervals = workloadProfile.profile.zip(durations).zip(requestGenerators).map( e => new WorkloadIntervalDescription(e._1._1,e._1._2,e._2) )
		new WorkloadDescription(thinkTimeMean,intervals)
	}
	
	def cat(workloads:List[WorkloadDescription]):WorkloadDescription = new WorkloadDescription(workloads(0).thinkTimeMean,List.flatten( workloads.map(_.workload) ))
	
	def deserialize(file: String): WorkloadDescription = {
        val fin = new ObjectInputStream( new FileInputStream(file) )
        val wd = fin.readObject().asInstanceOf[WorkloadDescription]
        fin.close
        wd
	}
}

//////
// Wikipedia dataset
//
object WikipediaDataset {
	val datasets = Map("wikipedia_20090623_20090630"->"http://scads.s3.amazonaws.com/workload/wikipedia_20090623_20090630.txt")
	
	def getTotalHits(hourURL:String):Int = {
		var sum = 0
		for(line<-Source.fromURL(hourURL).getLines) sum+=line.trim.split(',')(1).toInt
		sum
	}
	
	def loadPagesAndHits(hourURL:String, count:Int, filter:Set[String]):Map[String,Int] = {
		val br = new BufferedReader(new InputStreamReader(new URL(hourURL).openStream()))
		var done = false
		val pages = scala.collection.mutable.Map[String,Int]()
		while (!done) {
			val line = br.readLine
			if (line==null) done=true
			else {
				val s = line.trim.split(",")
				if (s.length>=2 && s(0)!="Total" && (filter==null || filter.contains(s(0))) ) pages(s(0)) = pages.getOrElse(s(0),0) + s(1).toInt
			}
		}
		val top = pages.keySet.toList.sort(pages(_)>pages(_)).take(count).foldLeft( scala.collection.mutable.Map[String,Int]())( (n,k)=>n+(k->pages(k)) )
		Map[String,Int]() ++ top
	}
}


//////
// workload profile
//
object WorkloadProfile {
	def getFlat(nintervals:Int, nusers:Int): WorkloadProfile = {
		new WorkloadProfile( List.make(nintervals,nusers) )
	}
	
	def getLinear(nintervals:Int, nusersStart:Int, nusersEnd:Int): WorkloadProfile = {
		val step = (nusersEnd.toDouble-nusersStart)/(nintervals-1)
		new WorkloadProfile( (1 to nintervals).map( (i:Int) => Math.round(nusersStart + (i-1)*step).toInt ).toList )
	}

	def getSpiked(nintervals:Int, nusersFlat:Int, nusersSpike:Int): WorkloadProfile = {
		new WorkloadProfile( List(nusersFlat,nusersSpike,nusersFlat) )
	}

	/**
	* The Ebates workload file is aggregated into 1-minute intervals. This method will skip the first 
	* 'skipMinutes' minutes, and will create 1 workload interval from 'intervalMinutes' of the data
	*/
	def getEbatesProfile(nintervals:Int, skipMinutes:Int, intervalMinutes:Int, maxUsers:Int): WorkloadProfile = {
		var w = new ListBuffer[Double]() ++ List.make(nintervals,0.0)
		getWorkloadFromURL("http://scads.s3.amazonaws.com/workload/ebates_may2003_1min.csv","workload")
					.drop(skipMinutes)
					.take(nintervals*intervalMinutes)
					.zipWithIndex
					.foreach( e => w(e._2/intervalMinutes)+=e._1 )
		val maxw = w.reduceLeft( Math.max(_,_) ).toDouble
		new WorkloadProfile( w.map( (x:Double) => (x/maxw*maxUsers).toInt ).toList )
	}
	
	private def getWorkloadFromURL(url:String, column:String): List[Double] = {
		val workload = new scala.collection.mutable.ListBuffer[Double]()
		var columnI = -1
		for (line <- Source.fromURL(url).getLines)
			if (columnI == -1) columnI = line.trim.split(",").findIndexOf(_==column)
			else workload += line.trim.split(",")(columnI).toDouble
		workload.toList
	}
	
	def getWikipedia(nintervals:Int, dataset:String, nHoursSkip:Int, nHoursDuration:Int, nMaxUsers:Int): WorkloadProfile = {
		val hours = Source.fromURL(WikipediaDataset.datasets(dataset)).getLines.toList.map(_.trim).drop(nHoursSkip).take(nHoursDuration)
		val raw = WorkloadProfile( hours.map(WikipediaDataset.getTotalHits(_)) )
		WorkloadProfile( raw.interpolate( Math.ceil(nintervals.toDouble/(nHoursDuration-1)).toInt ).profile.take(nintervals) ).scale(nMaxUsers)
	}
}

@serializable
case class WorkloadProfile(
	val profile: List[Int]
) {
	private def fraction(a:Double, b:Double, i:Double, min:Double, max:Double):Double = min + (max-min)*(i-a)/(b-a)
	private def interpolateTwo(a:Double, b:Double, n:Int):List[Int] = (0 to n).toList.map( (i:Int)=>Math.round(a + (b-a)*i/n).toInt )
	
	def addSpike(tRampup:Int, tSpike:Int, tRampdown:Int, tEnd:Int, magnitude:Double): WorkloadProfile = {
		val maxUsers = profile.reduceLeft( Math.max(_,_) ).toDouble
		val w = profile.zipWithIndex.map( e =>  	 if (e._2<tRampup) 				e._1.toDouble 
											else if (e._2>=tRampup&&e._2<tSpike) 	e._1 * fraction(tRampup,tSpike,e._2,1,magnitude).toDouble
											else if (e._2>=tSpike&&e._2<tRampdown) 	e._1 * magnitude.toDouble
											else if (e._2>=tRampdown&&e._2<tEnd) 	e._1 * fraction(tRampdown,tEnd,e._2,magnitude,1).toDouble
											else 				 					e._1.toDouble ).toList
		val maxw = w.reduceLeft( Math.max(_,_) ).toDouble
		WorkloadProfile( w.map( (x:Double) => (x/maxw*maxUsers).toInt ) )
	}
	
	def interpolate(nSegments:Int): WorkloadProfile = {
		WorkloadProfile( List.flatten( profile.dropRight(1).zip(profile.tail).map( (p:Tuple2[Int,Int]) => interpolateTwo(p._1,p._2,nSegments).dropRight(1) ) )+profile.last )
	}
	
	def scale(nMaxUsers:Int): WorkloadProfile = {
		val maxw = profile.reduceLeft( Math.max(_,_) ).toDouble
		WorkloadProfile( profile.map( (w:Int) => (w/maxw*nMaxUsers).toInt ) )
	}
	
	override def toString(): String = profile.toString()
}


//////
// workload mix
//
object WorkloadMixProfile {
	def getStaticMix(nintervals:Int, mix:MixVector): WorkloadMixProfile = new WorkloadMixProfile( List.make(nintervals,mix) )
}

@serializable
case class WorkloadMixProfile(
	val profile: List[MixVector]
) {
	def getProfile: List[MixVector] = profile
	def transition(that:WorkloadMixProfile, tStart:Int, tEnd:Int): WorkloadMixProfile =
		new WorkloadMixProfile( profile.zipWithIndex.map( m => m._1.transition(that.getProfile(m._2),if (m._2<tStart) 0 else if (m._2>tEnd) 1 else (m._2.toDouble-tStart)/(tEnd-tStart)) ) )
	override def toString() = profile.map(_.toString).mkString("MIX[",",","]")
}

@serializable
class MixVector(
	_mix: Map[String,Double]
) {
	val mix = normalize(_mix)
	
	private def sum(m:Map[String,Double]): Double = m.values.reduceLeft(_+_)
	private def normalize(m:Map[String,Double]): Map[String,Double] = m.transform((k,v)=>v/sum(m))
	def getMix: Map[String,Double] = mix
	
	def transition(that:MixVector,c:Double): MixVector = {
		new MixVector( mix.transform( (k,v) => t(v,that.mix(k),c) ) )
	}
	private def t(a:Double, b:Double, r:Double): Double = a + r*(b-a)
	override def toString() = mix.keySet.toList.sort(_<_).map(k=>k+"->"+mix(k)).mkString("m[",",","]")
	
	def sampleRequestType(): String = {
		val r = WorkloadDescription.rnd.nextDouble()
		var agg:Double = 0

		var reqType = ""
		for (req <- mix.keySet) {
			agg += mix(req)
			if (agg>=r && reqType=="") reqType = req
		}
		reqType
	}
}


//////
// key generators
//

object SCADSKeyGenerator {
	def getMinKey(gens:List[SCADSKeyGenerator]):Int = gens.map(_.minKey).reduceLeft(Math.min(_,_))
	def getMaxKey(gens:List[SCADSKeyGenerator]):Int = gens.map(_.maxKey).reduceLeft(Math.max(_,_))
	
	def wikipediaKeyProfile(nintervals:Int, dataset:String, nHoursSkip:Int, nHoursDuration:Int, nKeys:Int, keyHour:Int): List[SCADSKeyGenerator] = {
		val keyhourURL = Source.fromURL(WikipediaDataset.datasets(dataset)).getLines.toList.map(_.trim)(keyHour)
		val keys = Set[String]() ++ WikipediaDataset.loadPagesAndHits(keyhourURL, nKeys, null).keySet
		val hours = Source.fromURL(WikipediaDataset.datasets(dataset)).getLines.toList.map(_.trim).drop(nHoursSkip).take(nHoursDuration)
		val hourGenerators = hours.map( WikipediaKeyGenerator(_,keys) )	
		val nPerHour = Math.ceil(nintervals.toDouble/(nHoursDuration-1)).toInt
		val keyProfile = List.flatten( hourGenerators.dropRight(1).zip(hourGenerators.tail).map( (g)=> (0 to (nPerHour-1)).toList.map( (i:Int)=> MixtureKeyGenerator(Map(g._1->(1.0-i/nPerHour.toDouble),g._2->(i/nPerHour.toDouble)) )) ) )+hourGenerators.last
		keyProfile.take(nintervals)
	}
	
	def sampleKeys(generators:List[SCADSKeyGenerator], nsamples:Int, file:String) {
		val f = new BufferedWriter(new FileWriter(file))		
		for (gen <- generators)	f.write( (1 to nsamples).toList.map( i=>gen.generateKey ).mkString("",",","\n") )
		f.close
	}
	
}

@serializable
abstract class SCADSKeyGenerator(
	val minKey: Int,
	val maxKey: Int
) {
	def generateKey(): Int
}

@serializable
case class UniformKeyGenerator(
	override val minKey: Int,
	override val maxKey: Int
) extends SCADSKeyGenerator(minKey,maxKey) {
	override def generateKey(): Int = WorkloadDescription.rnd.nextInt(maxKey-minKey) + minKey
}

@serializable
case class MixtureKeyGenerator(
	components: Map[SCADSKeyGenerator,Double]
) extends SCADSKeyGenerator(SCADSKeyGenerator.getMinKey(components.keySet.toList),SCADSKeyGenerator.getMaxKey(components.keySet.toList)) {
	override def generateKey(): Int = {
		var (s,r) = (0.0,WorkloadDescription.rnd.nextDouble)
		(for (c<-components) yield {s+=c._2;(c._1,s)}).find(r<_._2).get._1.generateKey
	}
}

@serializable
case class ZipfKeyGenerator(
	val a: Double,
	override val minKey: Int,
	override val maxKey: Int
) extends SCADSKeyGenerator(minKey,maxKey) {
	assert(a>1, "need a>1")
	val r = WorkloadDescription.rnd.nextDouble
	
	override def generateKey(): Int = {
		var k = -1
		do { k=sampleZipf } while (k>maxKey)
		Math.abs( ((k+1)*r).hashCode ) % (maxKey-minKey) + minKey
	}
	
	private def sampleZipf(): Int = {
		val b = Math.pow(2,a-1)
		var u, v, x, t = 0.0
		do {
			u = WorkloadDescription.rnd.nextDouble()
			v = WorkloadDescription.rnd.nextDouble()
			x = Math.floor( Math.pow(u,-1/(a-1)) )
			t = Math.pow(1+1/x,a-1)
		} while ( v*x*(t-1)/(b-1)>t/b )
		x.toInt
	}
}

@serializable
case class WikipediaKeyGenerator(
	url: String,
	keys: Set[String]
) extends SCADSKeyGenerator(0,keys.size-1) {
	val (pages,cdf,pagehits) = initialize
	
	private def initialize():(Array[String],Array[Double],Map[String,Int]) = {
		val pagehits = WikipediaDataset.loadPagesAndHits(url, keys.size, keys)
		val pages = pagehits.keySet.toList.sort(_<_).toArray
		var sum = 0.0 
		val cdf = (for(page<-pages)yield{sum+=pagehits(page);sum}).toList.map(_/sum).toArray
		(pages,cdf,pagehits)
	}
	
	override def generateKey(): Int = {
		var (i0,i1,i) = (0,cdf.length-1,0)
		val r = WorkloadDescription.rnd.nextDouble
		
		var found = false
		while (!found) {
			i = (i0+i1)/2
			if ( (i==0&&r<=cdf(i)) || (cdf(i-1)<r&&r<=cdf(i)) ) found=true
			else if (r<=cdf(i-1)) i1=i-1
			else i0=i+1
		}
		i
	}
	def generatePage():String = pages(generateKey)
}


@serializable
case class EmpiricalKeyGenerator(
	val url: String,
	override val minKey: Int,
	override val maxKey: Int
) extends SCADSKeyGenerator(minKey,maxKey) {
	val (pages,cdf) = initialize()
	
	private def initialize(): (Array[String], Array[Double]) = {
		val cdf = new ListBuffer[Double]()
		val pages = new ListBuffer[String]()
		
//		val br = new BufferedReader(new InputStreamReader(new FileInputStream(new File("/Users/bodikp/workspace/data/wikipedia/wikipedia-pagecounts-20090501-020000.csv"))))
		val br = new BufferedReader(new InputStreamReader(new URL(url).openStream()))
		var done = false
		var line = ""

		var sum = 0.0
		var count = 0

		while (!done) {
			count += 1
			if (count%10000==0) println(count + "  "+pages.last+ "  "+cdf.last)
						
			line = br.readLine
			if (line==null) done=true
			else {
				val s = line.trim.split(",")
				if (s.length>=2) {
					val (page,count) = (s(0), s(1).toInt)
					if (page!="Total") {
						pages += page
						sum += count
						cdf += sum
					}
				} else println("skipping: "+line.trim)
			}
			if (count>maxKey-minKey) done=true  // stop if we have enough keys
		}
		(pages.toArray,cdf.map(_/sum).toArray)
	}
	
	override def generateKey(): Int = {
		var (i0,i1) = (0,cdf.length-1)
		var i,c = 0
		val r = WorkloadDescription.rnd.nextDouble
		
		var found = false
		while (!found) {
			c += 1
			i = (i0+i1)/2
			if ( (i==0&&r<=cdf(i)) || (cdf(i-1)<r&&r<=cdf(i)) ) found=true
			else if (r<=cdf(i-1)) i1=i
			else i0=i
		}
		i+minKey
	}
}

//////
// SCADS requests
//
abstract class SCADSRequest(
	val client: ClientLibrary
) {
	def reqType: String
	def execute
}

case class SCADSGetRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val key: String
) extends SCADSRequest(client) {
	def reqType: String = "get"
	def execute = {
		val value = client.get(namespace,key).value
		value
	}
	override def toString: String = "get("+namespace+","+key+")"
}

case class SCADSPutRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val key: String,
	val value: String
) extends SCADSRequest(client) {
	def reqType: String = "put"
	def execute = {
		val success = client.put(namespace,new Record(key,value))
		success
	}
	override def toString: String = "put("+namespace+","+key+"="+value+")"
}

case class SCADSGetSetRequest(
	override val client: ClientLibrary,
	val namespace: String,
	val startKey: String,
	val endKey: String,
	val skip: int,
	val limit: int
) extends SCADSRequest(client) {
	def reqType: String = "getset"
	def execute = {
		client.get_set(namespace,new RecordSet(3,new RangeSet("'"+startKey+"'","'"+endKey+"'",skip,limit),null,null))
	}
}


//////
// request generators
//
@serializable
abstract class SCADSRequestGenerator(
	val mix: MixVector
) {
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest
}


@serializable
class SimpleSCADSRequestGenerator(
	override val mix: MixVector,
	val parameters: Map[String, Map[String, String]]
) extends SCADSRequestGenerator(mix) {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	
	val getKeyGenerator = new UniformKeyGenerator(parameters("get")("minKey").toInt,parameters("get")("maxKey").toInt)
	val getNamespace = parameters("get")("namespace")
	
	val putKeyGenerator = new UniformKeyGenerator(parameters("put")("minKey").toInt,parameters("put")("maxKey").toInt)
	val putNamespace = parameters("put")("namespace")
	
	val getsetKeyGenerator = new UniformKeyGenerator(parameters("getset")("minKey").toInt,parameters("getset")("maxKey").toInt)
	val getsetNamespace = parameters("getset")("namespace")
	val getsetSetLength = parameters("getset")("setLength").toInt
	
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		mix.sampleRequestType match {
			case "get" => new SCADSGetRequest(client,getNamespace,keyFormat.format(getKeyGenerator.generateKey))
			case "put" => new SCADSPutRequest(client,putNamespace,keyFormat.format(putKeyGenerator.generateKey),"value")
			case "getset" => {
				val startKey = getsetKeyGenerator.generateKey.toInt
				val endKey = startKey+getsetSetLength
				new SCADSGetSetRequest(client,getsetNamespace,keyFormat.format(startKey),keyFormat.format(endKey),0,getsetSetLength)
			}
		}
	}
}

@serializable
case class FixedSCADSRequestGenerator(
	override val mix: MixVector,
	val keyGenerator: SCADSKeyGenerator,
	val namespace: String,
	val getsetRangeLength: Int
) extends SCADSRequestGenerator(mix) {
	val keyFormat = new java.text.DecimalFormat("000000000000000")
	
	def generateRequest(client: ClientLibrary, time: Long): SCADSRequest = {
		mix.sampleRequestType match {
			case "get" => new SCADSGetRequest(client,namespace,keyFormat.format(keyGenerator.generateKey))
			case "put" => new SCADSPutRequest(client,namespace,keyFormat.format(keyGenerator.generateKey),"value")
			case "getset" => {
				val startKey = keyGenerator.generateKey
				val endKey = startKey+getsetRangeLength
				new SCADSGetSetRequest(client,namespace,keyFormat.format(startKey),keyFormat.format(endKey),0,getsetRangeLength)
			}
		}
	}
}