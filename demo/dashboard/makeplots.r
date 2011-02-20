#chmod 775 /usr/local/lib/R/site-library or whatever
#install.packages("RMySQL")

#dbListTables(con)
#dbReadTable(con, "appReqRate")
#locator(1)


###INTRO

library(RMySQL)
con <- dbConnect(MySQL(), dbname = "radlabmetrics" , user="radlab_dev", password="randyAndDavelab", host="dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com")

TZShift = 8*3600	#PST = GMT - 8 hours
PERIODS = c(10, 60, 300)	#minutes
WEBAPP_STATS = TRUE
SCADS_STATS = TRUE
PIQL_STATS = TRUE
RAIN_STATS = TRUE

mostRecentPt <- function(tableName) {
	val <- dbGetQuery(con,paste("select timestamp from ",tableName," order by timestamp DESC limit 1", sep=''))
	val[[1]]
}

mostRecent = max(mostRecentPt("appReqRate"),mostRecentPt("namespaceReqRate"), mostRecentPt("piqlReqRate"), mostRecentPt("rainStats"))
print(paste("Most recent time is",mostRecent))

###### Rain stats
if(RAIN_STATS) {
	for(period in PERIODS) {
		startT = (mostRecent - 1000* 60 * period -1) #- 60*1000*2
	
		series <- dbGetQuery(con, paste("select timestamp,totalResponseTime,operationsSuccessful,actionsSuccessful from rainStats where timestamp > ",startT," order by timestamp DESC ",sep=''))

		if (nrow(series) > 0) {
			reqRatePlotFile=paste("rainReqRate-",period,".png",sep='')
			png(file=reqRatePlotFile, bg="transparent")
		
			times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
		
			respTime <- series[TRUE,2]
			opVec = respTime/series[TRUE,3]
	#		actVec = respTime/series[TRUE,4]
	#		allPts = c(opVec,actVec)
	#		yrange <- range(allPts[ !is.nan(allPts)])
			yrange <- range(opVec[ !is.nan(opVec)])
			yrange[1] = 0
			print(paste("have",length(opVec),"data points for RAIN; series length was ",length(series[[1]])))
			plot(times, opVec , ylab="avg time (ms)", xlab="time", col="red",	 xaxt="n", type="o", main="",ylim=yrange,cex.lab=1.2)
			axis.POSIXct(1, times, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
			mtext("RAIN avg response time",side=3,cex=1.4,line=2)

			par(new=F)
		
		# legend( x="topleft", inset=0.05, c("operations", "Actions"), cex=1.0, col=c("red","blue"), bg="white", pch=21:22, lty=1:2)
			legend( x="topleft", inset=0.05, "operations", cex=1.2, col="red", bg="white", pch=21:22, lty=1:2)
			dev.off()
		}
	}
}

############# Web App stats
if(WEBAPP_STATS) {
	for(period in PERIODS) {

		# plot web server load, allocation data for SCADr
		startT = (mostRecent - 1000* 60 * period -1)
		series <- dbGetQuery(con,paste("select timestamp,aggRequestRate,targetNumServers from appReqRate where webAppID = 'SCADr' and timestamp > ",startT," order by timestamp DESC ",sep=''))
		
		if (nrow(series) > 0) {
			reqRatePlotFile=paste("scadrReqRate-",period,".png",sep='')
			png(file=reqRatePlotFile, bg="transparent")
		
			times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
			par(oma=c(1,1,1,2))
			
			yrange = c(0, 1.2*max(series[TRUE,2]))
			plot(times, series[TRUE,2], xlab="", ylab="requests per second", col="red", xaxt="n", type="o", main="",col.lab="red",cex.lab=1.2, ylim=yrange)
			axis.POSIXct(1, times, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		
			yrange = c(0, ceiling(1.2*max(series[TRUE,3])))
			par(new=T)
			plot(times, series[TRUE,3] , col="blue",axes=F,xlab="",ylab="",type="o",ylim=yrange)
		
			axis(side=4, at=yrange,cex=1.2)
			mtext("servers",side=4,col="blue",line=1,cex=1.2)
			mtext("time", side=1,line=3,cex=1.2)
			mtext("Web server load and allocation for SCADr",side=3,cex=1.4,line=2)

			legend( x="topleft", inset=0.05, c("Request rate","Target server count"), cex=1.0, col=c("red","blue"), bg="white", pch=21:22, lty=1:2)
			dev.off()
		}


		# plot web server load, allocation data for gRADit
		startT = (mostRecent - 1000* 60 * period -1)
		series <- dbGetQuery(con,paste("select timestamp,aggRequestRate,targetNumServers from appReqRate where webAppID = 'gRADit' and timestamp > ",startT," order by timestamp DESC ",sep=''))
		
		if (nrow(series) > 0) {
			reqRatePlotFile=paste("graditReqRate-",period,".png",sep='')
			png(file=reqRatePlotFile, bg="transparent")
		
			times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
			par(oma=c(1,1,1,2))
			
			yrange = c(0, 1.2*max(series[TRUE,2]))
			plot(times, series[TRUE,2], xlab="", ylab="requests per second", col="red", xaxt="n", type="o", main="",col.lab="red",cex.lab=1.2, ylim=yrange)
			axis.POSIXct(1, times, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		
			yrange = c(0, ceiling(1.2*max(series[TRUE,3])))
			par(new=T)
			plot(times, series[TRUE,3] , col="blue",axes=F,xlab="",ylab="",type="o",ylim=yrange)
		
			axis(side=4, at=yrange,cex=1.2)
			mtext("servers",side=4,col="blue",line=1,cex=1.2)
			mtext("time", side=1,line=3,cex=1.2)
			mtext("Web server load and allocation for gRADit",side=3,cex=1.4,line=2)

			legend( x="topleft", inset=0.05, c("Request rate","Target server count"), cex=1.0, col=c("red","blue"), bg="white", pch=21:22, lty=1:2)
			dev.off()
		}

		
		# plot avg CPU utilization 
		cpuSeries = dbGetQuery(con,paste("select timestamp,averageUtilization from appReqRate where webAppID = 'SCADr' and timestamp > ",startT," order by timestamp DESC ",sep=''))
		if (nrow(cpuSeries) > 0)) {
			cpuPlotFile=paste("averageCpuUtilization-",period,".png",sep='')
			png(file= cpuPlotFile, bg="transparent")
		
			times <- as.POSIXct( (cpuSeries[TRUE,1]/1000 - TZShift), origin="1970-01-01")
			par(oma=c(1,1,1,2))
			
			yrange = c(0, 1.2*max(cpuSeries[TRUE,2], na.rm=TRUE))
			plot(times, cpuSeries[TRUE,2], xlab="", ylab="Average CPU utilization", col="red", xaxt="n", type="o", main="",cex.lab=1.2, ylim=yrange)
			
						
			
			axis.POSIXct(1, times, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
			mtext("time", side=1,line=3,cex=1.2)
			mtext("Web-server average CPU allocation",side=3,cex=1.4,line=2)
		}
	}
} #end webapp stats

############# PIQL stats
if(PIQL_STATS) {
	for(period in PERIODS) {
		startT = (mostRecent - 1000* 60 * period -1)
	
		series <- dbGetQuery(con, paste("select timestamp, sum(aggRequestRate) from piqlReqRate where timestamp > ",startT," group by timestamp order by timestamp DESC",sep=''))
		
		if (nrow(series) == 0)
			continue

		times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
		

		png(file=paste("piqlReqRate-",period,".png",sep=''), bg="transparent")
	
		yrange = c(0,1.2*max(series[TRUE,2]))
		plot(times, series[TRUE,2], ylab="requests per second", xlab="time", col="red",	 xaxt="n", type="o", main="",cex.lab=1.2, ylim=yrange)
		axis.POSIXct(1, times, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		mtext("PIQL request rate",side=3,cex=1.4,line=2)

		dev.off()
		
		
		png(file=paste("piql99thPercentile-",period,".png",sep=''), bg="transparent")
		series <- dbGetQuery(con, paste("select timestamp,respTime99th from piqlReqRate where timestamp > ",startT," order by timestamp DESC",sep=''))
		yrange = c(0,1000)
		distinctTime <- unique(series[TRUE,1])
		medians = rep(0,length(distinctTime))
		for(i in 1:length(distinctTime)) {
		  thisTimeSubset = series[ series[TRUE,1] == distinctTime[i]  ,2]
		  medians[i] = median(thisTimeSubset)
		  }
		multiTimes <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
    distinctTimes <- as.POSIXct( (distinctTime/1000 - TZShift), origin="1970-01-01")
		plot(multiTimes, series[TRUE,2], ylab="99th percentile latency (ms)", xlab="time", col="red",	 xaxt="n", type="p", main="",cex.lab=1.2, ylim=yrange)
		par(new=T)
		plot(distinctTimes, medians, col="blue",axes=F,xlab="",ylab="",type="o",ylim=yrange)

		axis.POSIXct(1, distinctTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		mtext("PIQL 99th Percentiles by servers",side=3,cex=1.4,line=2)
		legend( x="topleft", inset=0.01, c("Per-server","Median-of-servers"), cex=1.0, col=c("red","blue"), bg="white", pch=21:22, lty=1:2)

		dev.off()

		
	}
}

############# SCADS stats
if(SCADS_STATS) {
	for(period in PERIODS) {
		startT = (mostRecent - 1000* 60 * period -1)
	
	#First get the data...
		namespaceNamesL <- dbGetQuery(con, paste("select DISTINCT namespace from namespaceReqRate where timestamp > ",
						 startT," order by namespace",sep=''))
		namespaceNames = namespaceNamesL[[1]]
		dataByNamespace = vector("list", dim(namespaceNamesL)[1])
	
		i = 0
		maxReqs = maxServers = 0
		for(namespace in namespaceNames) {
			i = i + 1
			series <- dbGetQuery(con, paste("select timestamp,aggRequestRate,numServers from namespaceReqRate where timestamp > ",startT," and namespace = \"", namespace ,"\" order by timestamp DESC ",sep=''))
			dataByNamespace[[i]] = series
			maxReqs = max(maxReqs, max(series[TRUE,2]))
			maxServers = max(maxServers, max(series[TRUE,3]))
		}
		rawAXTime <- dbGetQuery(con, paste("select DISTINCT timestamp from namespaceReqRate where timestamp > ",
					 startT," order by timestamp DESC",sep=''))
	
		axTimes = as.POSIXct( ( rawAXTime[TRUE,1]/1000 - TZShift), origin="1970-01-01")
	
	## Now plot
		reqRatePlotFile=paste("scadsReqRate-",period,".png",sep='')
		png(file=reqRatePlotFile, bg="transparent")
	
		yrange<- c(0, maxReqs * 1.1)

		i = 0
		for(namespace in namespaceNames) {
			i = i+1		
			series = dataByNamespace[[i]]
			times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
			par(new=T)
			
			if (nrow(series) > 0) {
				plot(times, series[TRUE,2], ylab="", xlab="", col=i, xaxt="n", type="o", main="",ylim=yrange)
			}
		}
		axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
	# axis(2, series[TRUE,2])
		mtext("SCADS request rate",side=3,cex=1.4,line=2)
		mtext("requests per second", side=2, line =3,cex=1.2)
		mtext("time", side=1, line =2.5,cex=1.2)

		legend( x="topleft", inset=0.05, as.character(namespaceNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:2)
	
		par(new=F)
		dev.off()
	
	##Now for machines
		yrange<- c(0, maxServers * 1.1)
		reqRatePlotFile=paste("scadsMachineAlloc-",period,".png",sep='')
		png(file=reqRatePlotFile, bg="transparent")
			i = 0
		for(namespace in namespaceNames) {
			i = i+1		
			series = dataByNamespace[[i]]
				times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
			par(new=T)
			
			if (nrow(series) > 0) {
				plot(times, series[TRUE,3], ylab="", xlab="", col=i,	xaxt="n", type="o", main="",ylim=yrange,cex=1.2)
			}
		}
		axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		mtext("SCADS server allocations",side=3,cex=1.4,line=2)
		mtext("time", side=1, line =2.5,cex=1.2)
		mtext("servers", side=2, line =3,cex=1.2)
	
		legend( x="topleft", inset=0.05, as.character(namespaceNames), cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:2)
		par(new=F)
		dev.off()
	}
}

