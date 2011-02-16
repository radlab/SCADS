library(RMySQL)
con <- dbConnect(MySQL(), dbname = "radlabmetrics" , user="radlab_dev", password="randyAndDavelab", host="dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com")

TZShift = 8*3600  #PST = GMT - 8 hours
PERIODS = c(10, 60, 300)  #miuntes
plotRelShares = FALSE
#
mostRecentPt <- function(tableName) {
  val <- dbGetQuery(con,paste("select timestamp from ",tableName," order by timestamp DESC limit 1", sep=''))
  val[[1]]
}

mergeIn <- function(fullK, partK, fullV, partV) {
	j = 1
	print(paste("merging fullK,fullV of lengths",length(fullK), length(fullV), "with partK,partV of lengths", length(partK), length(partV)))
#invariant:  partK[j] <= fullK[i], elements up to fullK[i] have been merged
	for(i in 1:length(fullK)) {
#	  print(paste("i =",i,"j=",j, "fullK[i]=",fullK[i],"partK[j]=",partK[j]))
		if(j > length(partK))
		  break
		while(partK[j] > (fullK[i]) && j <= i) {
			j = j+1
		}
		if(j > i)
		  print(paste("WARN: j got ahead of i in mergeIn; i =",i,"j=",j))
		if(partK[j] == fullK[i]) {
			fullV[i] = fullV[i] + partV[j]
			j = j+1
		}
	}
	fullV
}

###### Mesos Stats

mostRecent = mostRecentPt("mesosStats")

for(period in PERIODS) {

	tsBase = (mostRecent - 1000* 60 * period -1)


frameworkNamesL <- dbGetQuery(con, paste("select DISTINCT frameworkName from mesosStats where timestamp > ",
	       tsBase," order by frameworkName",sep=''))
   
frameworkNames = frameworkNamesL[[1]]

#Note that dataByFramework indexes start from 1, not 0
	tsBase = mostRecent - 1000* 60 * period -1
	rawAXTime <- dbGetQuery(con, paste("select DISTINCT timestamp from mesosStats where timestamp > ",
	       tsBase," order by timestamp DESC",sep=''))

	axTimes = as.POSIXct( ( rawAXTime[TRUE,1]/1000 - TZShift), origin="1970-01-01")
	dataByFramework = vector("list", dim(frameworkNamesL)[1])
	times = vector("list", dim(frameworkNamesL)[1])
	i = 1
	for(framework in frameworkNames) {
		print(paste("querying for data from framework",framework," with id=",i))
		datForFramework <- dbGetQuery(con, paste("select timestamp,CPUs,GBRam,CPUShare,memShare,domShare from mesosStats where frameworkName = '",framework,"' and timestamp > ", tsBase," order by timestamp DESC",sep=''))
		POINTS = dim(datForFramework)[1]
		dataByFramework[[i]] = datForFramework
		if(POINTS > 0) {
			times[[i]] = as.POSIXct( ( (dataByFramework[[i]])[1:POINTS,1]/1000 - TZShift), origin="1970-01-01")
		}
		i = i+1
	}
	
	###End mesos preliminaries
	if(plotRelShares) {
		reqRatePlotFile=paste("mesosCPUShares",period,".png",sep='')
		png(file=reqRatePlotFile, bg="transparent")
		
		#par(new=T)
		yrange<-c(0,1.2)
		i =0
		runningTotalVec = rep(0,length(axTimes))
		for(framework in frameworkNames) {
			i = i +1
			POINTS = dim(dataByFramework[[i]])[1]
			print(paste("plotting CPU share for",framework, "with",POINTS,"points"))
			if(POINTS == 0) {
				next;
			}
			
			par(new=T)
			runningTotalVec = mergeIn(axTimes, times[[i]],runningTotalVec, dataByFramework[[i]][1:POINTS,4])
			plot(axTimes, runningTotalVec, ylab="CPU share [cumulative]", xlab="time", col=i,  xaxt="n", type="b", main="Mesos CPU shares",ylim=yrange,cex=1.2)
		}
		axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		
		legend( x="topleft", inset=0.05, as.character(frameworkNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:i)
		
		dev.off()
		reqRatePlotFile=paste("mesosMemShares",period,".png",sep='')
		png(file=reqRatePlotFile, bg="transparent")
		
		
		yrange<-c(0,1.2)
		i =0
		runningTotalVec = rep(0,length(axTimes))
		for(framework in frameworkNames) {
			i = i +1
			POINTS = dim(dataByFramework[[i]])[1]
			print(paste("plotting mem share for",framework, "with",POINTS,"points"))
			if(POINTS == 0) {
				 next;
			}
			
			par(new=T)
			runningTotalVec = mergeIn(axTimes, times[[i]],runningTotalVec, dataByFramework[[i]][1:POINTS,5])
			plot(axTimes, runningTotalVec, ylab="Mem share [cumulative]", xlab="time", col=i,  xaxt="n", type="o", main="Mesos memory shares",ylim=yrange,cex.lab=1.2)
		}
		axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		
		legend( x="topleft", inset=0.05, as.character(frameworkNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:i)
		dev.off()
	}  #end plot rel shares
	
#######CPU shares [absolute]

	reqRatePlotFile=paste("mesosCPUAbsShares",period,".png",sep='')
	png(file=reqRatePlotFile, bg="transparent")

	maxpt = 0;
	for(i in 1:length(dataByFramework))
		maxpt = max(maxpt, max(dataByFramework[[i]][TRUE,2]))

	yrange<-c(0,maxpt*1.2)
	
	i =0		
	for(framework in frameworkNames) {
		i = i +1
		POINTS = dim(dataByFramework[[i]])[1]
		print(paste("plotting CPU totals for",framework, "with",POINTS,"points"))
		if(POINTS == 0) {
		   next;
		}
		times <- as.POSIXct( (dataByFramework[[i]][TRUE,1]/1000 - TZShift), origin="1970-01-01")

		par(new=T)
		plot(times, dataByFramework[[i]][1:POINTS,2], ylab="", xlab="time", col=i,  xaxt="n", type="o",main="",ylim=yrange,cex.lab=1.2)
	}
	axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		mtext("Mesos cpu allocations [cores]",side=3,cex=1.4,line=2)
	mtext("Cores",side=2,cex=1.2,line=2.5)

	legend( x="topleft", inset=0.01, as.character(frameworkNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:i)
	dev.off()
	
	
#######Mem shares [absolute]

	reqRatePlotFile=paste("mesosMemAbsShares",period,".png",sep='')
	png(file=reqRatePlotFile, bg="transparent")

	maxpt = 0;
	for(i in 1:length(dataByFramework))
		maxpt = max(maxpt, max(dataByFramework[[i]][TRUE,3]))

	yrange<-c(0,maxpt*1.2)
	
	i =0		
	for(framework in frameworkNames) {
		i = i +1
		POINTS = dim(dataByFramework[[i]])[1]
		print(paste("plotting mem totals for",framework, "with",POINTS,"points"))
		if(POINTS == 0) {
		   next;
		}
		times <- as.POSIXct( (dataByFramework[[i]][TRUE,1]/1000 - TZShift), origin="1970-01-01")

		par(new=T)
		plot(times, dataByFramework[[i]][TRUE,3], ylab="", xlab="time", col=i,  xaxt="n", type="o",ylim=yrange,cex.lab=1.2)
	}
	axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
	legend( x="topleft", inset=0.01, as.character(frameworkNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:i)
	mtext("Mesos memory allocations [GB]",side=3,cex=1.4,line=2)
	mtext("Mem allocation (GB)",side=2,cex=1.2,line=2.5)

	dev.off()

}   #end loop over periods