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
		startT <- (mostRecent - 1000* 60 * period -1) 
		tracknamesL <- dbGetQuery(con, paste("select DISTINCT trackName from rainStats where timestamp > ",startT," order by trackName",sep=''))
		
		if (nrow(tracknamesL) > 0)  {
			
		tracknames = tracknamesL[[1]]

		appNum = 0
		reqRatePlotFile=paste("rainReqRate-",period,".png",sep='')
		png(file=reqRatePlotFile, bg="transparent")
		allTimesRaw = dbGetQuery(con, paste("select DISTINCT timestamp from rainStats where timestamp > ",startT," order by timestamp",sep=''))
		allTimesPosix = as.POSIXct( ( allTimesRaw[TRUE,1]/1000 - TZShift), origin="1970-01-01")
		
		maxYTuple = dbGetQuery(con, paste("select totalResponseTime,operationsSuccessful from rainStats where timestamp > ",startT," order by totalResponseTime/operationsSuccessful DESC limit 1",sep=''))
		maxY = maxYTuple[[1]]/maxYTuple[[2]]
		yrange <- c(0,maxY * 1.2) 

		for(trackname in tracknames) {
			appNum = appNum+1
			series <- dbGetQuery(con, paste("select timestamp,totalResponseTime,operationsSuccessful,actionsSuccessful from rainStats where trackName = '",trackname,"' and timestamp > ",startT," order by timestamp ",sep=''))
			if (nrow(series) == 0)
				continue;
		
			times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
		
			respTime <- series[TRUE,2]
			opVec = respTime/series[TRUE,3]
			print(paste("have",length(opVec),"data points for RAIN; series length was ",length(series[[1]])))
			par(new=T)
			plot(times, opVec , ylab="avg time (ms)", xlab="time", col=appNum, xaxt="n", type="o", main="",ylim=yrange,cex.lab=1.2)
		}
		axis.POSIXct(1, allTimesPosix, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
		mtext("RAIN avg response times",side=3,cex=1.4,line=2)

		legend( x="topleft", inset=0.05, as.character(tracknames), cex=1.2, col=1:appNum, bg="white", pch=21:22, lty=1:2)
		dev.off()
		}
	}
}

############# Web App stats
if(WEBAPP_STATS) {
		appIDs = c("SCADr","gRADit","comRADes")

	for(period in PERIODS) {
	
	

		# plot web server load, allocation data for each app
		for(appID in appIDs) {		
			startT = (mostRecent - 1000* 60 * period -1)
			series <- dbGetQuery(con,paste("select timestamp,aggRequestRate,targetNumServers from appReqRate where webAppID = '",appID,"' and timestamp > ",startT," order by timestamp DESC ",sep=''))
			
			if (nrow(series) > 0) {
				reqRatePlotFile=paste(tolower(appID),"ReqRate-",period,".png",sep='')
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
				mtext(paste("Web server load and allocation for"appID),side=3,cex=1.4,line=2)
	
				legend( x="topleft", inset=0.05, c("Request rate","Server count"), cex=1.0, col=c("red","blue"), bg="white", pch=21:22, lty=1:2)
				dev.off()
			}
		}
		
		# plot avg CPU utilizations
		allTimesRaw = dbGetQuery(con, paste("select DISTINCT timestamp from appReqRate where timestamp > ",startT," order by timestamp",sep=''))
		allTimesPosix = as.POSIXct( ( allTimesRaw[TRUE,1]/1000 - TZShift), origin="1970-01-01")

		maxYTuple = dbGetQuery(con, paste("select averageUtilization from appReqRate where timestamp > ",startT," order by averageUtilization DESC limit 1",sep=''))
		yrange <- c(0,maxYTuple[[1]] * 1.2) 

		cpuPlotFile=paste("averageCpuUtilization-",period,".png",sep='')
		png(file= cpuPlotFile, bg="transparent")
		appNum = 0
		for(appID in appIDs) {		
			appNum = appNum + 1
			cpuSeries = dbGetQuery(con,paste("select timestamp,averageUtilization from appReqRate where webAppID = '",appID,"' and timestamp > ",startT," order by timestamp DESC ",sep=''))
			if (nrow(cpuSeries) > 0) {
			
				times <- as.POSIXct( (cpuSeries[TRUE,1]/1000 - TZShift), origin="1970-01-01")
				par(oma=c(1,1,1,2))
				par(new=T)
				plot(times, cpuSeries[TRUE,2], xlab="", ylab="", col=appNum, xaxt="n", type="o", main="",cex.lab=1.2, ylim=yrange)
			
		}
	}
	axis.POSIXct(1, allTimesPosix, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
	mtext("time", side=1,line=3,cex=1.2)
	mtext("Average CPU utilization",side=2,cex=1.2)
	mtext("Web server average CPU allocation",side=3,cex=1.4,line=2)
	legend( x="topleft", inset=0.05, as.character(appIDs), cex=1.0, col=1:appNum, bg="white", pch=21:22, lty=1:2)

	}  #end loop over periods
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
			thisTimeSubset = series[ series[TRUE,1] == distinctTime[i]	,2]
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
	
	appIDs = c("SCADr","gRADit","comRADes")
	for(appID in appIDs) {
		print(paste("starting on",appID))
	
		#First get the data...
			namespaceNamesL <- dbGetQuery(con, paste("select DISTINCT namespace from namespaceReqRate where timestamp > ",
							 startT," and appName = '",appID, "' order by namespace",sep=''))
			if(length(namespaceNamesL) > 0) {
				
			namespaceNames = namespaceNamesL[[1]]
			dataByNamespace = vector("list", dim(namespaceNamesL)[1])
				print(paste("found ", length(dataByNamespace), "distinct namespaces for",appID))

			shortNames = rep(0,length(namespaceNames))
			splitNames = strsplit(namespaceNames,'_')
			for(i in 1:length(namespaceNames)) 
				shortNames[i] = splitNames[[i]][1]
			shortNames = as.character(shortNames)
			print(paste("successfully computed short names, length =",length(shortNames)))
			i = 0
			maxReqs = maxServers = 0
			for(namespace in namespaceNames) {
				i = i + 1
				series <- dbGetQuery(con, paste("select timestamp,aggRequestRate,numServers from namespaceReqRate where timestamp > ",startT," and namespace = \"", namespace ,"\" and appName = '",appID, "' order by timestamp DESC ",sep=''))
				dataByNamespace[[i]] = series
				maxReqs = max(maxReqs, max(series[TRUE,2]))
				maxServers = max(maxServers, max(series[TRUE,3]))
			}
			rawAXTime <- dbGetQuery(con, paste("select DISTINCT timestamp from namespaceReqRate where timestamp > ",
						 startT," and appName = '",appID, "' order by timestamp DESC",sep=''))
			print("done with queries")
		
			axTimes = as.POSIXct( ( rawAXTime[TRUE,1]/1000 - TZShift), origin="1970-01-01")
		
		## Now plot
			reqRatePlotFile=paste("scadsReqRate-",appID,"-",period,".png",sep='')
			png(file=reqRatePlotFile, bg="transparent")
		
			yrange<- c(0, maxReqs * 1.1)
	
			i = 0
			for(namespace in namespaceNames) {
				print(paste("plotting req rate for namespace",namespace))

				i = i+1		
				series = dataByNamespace[[i]]
				
				if (nrow(series) > 0) {
					times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
					par(new=T)
					plot(times, series[TRUE,2], ylab="", xlab="", col=i, xaxt="n", type="o", main="",ylim=yrange)
				}
			}
			axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
			mtext(paste("SCADS request rate:",appID),side=3,cex=1.4,line=2)
			mtext("requests per second", side=2, line =3,cex=1.2)
			mtext("time", side=1, line =2.5,cex=1.2)
	
			legend( x="topleft", inset=0.05, shortNames, cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:2)
		
			par(new=F)
			dev.off()
		
		##Now for machines
			yrange<- c(0, maxServers * 1.1)
			reqRatePlotFile=paste("scadsMachineAlloc-",appID,"-",period,".png",sep='')
			png(file=reqRatePlotFile, bg="transparent")
				i = 0
			for(namespace in namespaceNames) {
				print(paste("plotting allocation for namespace",namespace))

				i = i+1		
				series = dataByNamespace[[i]]
				if (nrow(series) > 0) {
					times <- as.POSIXct( (series[TRUE,1]/1000 - TZShift), origin="1970-01-01")
					par(new=T)
					plot(times, series[TRUE,3], ylab="", xlab="", col=i,	xaxt="n", type="o", main="",ylim=yrange,cex=1.2)
				}
			}
			axis.POSIXct(1, axTimes, format="%Y-%m-%d %H:%M:%S", labels = TRUE)
			print("plotting legend")
			mtext(paste("SCADS server allocations:",appID),side=3,cex=1.4,line=2)
			mtext("time", side=1, line =2.5,cex=1.2)
			mtext("servers", side=2, line =3,cex=1.2)
		
			legend( x="topleft", inset=0.05, shortNames, cex=1.0, col=1:i, bg="white", pch=21:22, lty=1:2)
			par(new=F)
			dev.off()
			}
		} #end per appID
	} #end per time period
}

dbDisconnect(con)