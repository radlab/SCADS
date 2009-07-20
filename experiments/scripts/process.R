rename.col = function(data, name, new.name) {
	names(data)[names(data)==name] = new.name
	return(data)
}
make.col = function(data,col_def,name) {
	data[name]<-col_def
	return(data)
}

read.csvfile = function(filename) { # read csv file with heading
	data <- read.csv(file=filename,head=TRUE,sep=",")
	data<-na.omit(data)
	return(data)
}

process = function(data, interval=1000000,report_fraction=1) {
	workload = aggregate( data[,"start",drop=F], by=list(time=floor(data$start/interval)), FUN=length)
	workload = rename.col(workload,"start","workload")

	throughput = aggregate( data[,"end",drop=F], by=list(time=floor(data$end/interval)), FUN=length)
	throughput = rename.col(throughput,"end","throughput")

	if (!('latency' %in% colnames(data))) {
		data = make.col(data,data[,"end",drop=F]-data[,"start",drop=F],"latency")
	}
	
	cat("start latency")
	latency.mean = aggregate( data[,"latency",drop=F], by=list(time=floor(data$end/interval)), FUN=mean)
	latency.mean = rename.col(latency.mean,"latency","latency.mean")
	cat(".")
	latency.90p = aggregate( data[,"latency",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.9))})
	latency.90p = rename.col(latency.90p,"latency","latency.90p")
	cat(".")
	latency.99p = aggregate( data[,"latency",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.99))})
	latency.99p = rename.col(latency.99p,"latency","latency.99p")
	cat(".")
	latency.99.9p = aggregate( data[,"latency",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.999))})
	latency.99.9p = rename.col(latency.99.9p,"latency","latency.99.9p")
	cat(".")	
	stats = merge(workload,throughput,all=T)
	stats = merge(stats,latency.mean,all=T)
	stats = merge(stats,latency.90p,all=T)
	stats = merge(stats,latency.99p,all=T)
	stats = merge(stats,latency.99.9p,all=T)
	
	stats$workload = stats$workload * (1/report_fraction)
	stats$throughput = stats$throughput * (1/report_fraction)
	
	cat(".\n")
		
	# get quantiles for the client to server time, and server exec time
	if ('client_to_server' %in% colnames(data)) {
		cat("start client\n")
		client.mean = aggregate(data[,"client_to_server",drop=F],by=list(time=floor(data$end/interval)), FUN=mean)
		client.mean = rename.col(client.mean,"client_to_server","client.mean")
	
		client.90p = aggregate( data[,"client_to_server",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.9))})
		client.90p = rename.col(client.90p,"client_to_server","client.90p")
	
		client.99p = aggregate( data[,"client_to_server",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.99))})
		client.99p = rename.col(client.99p,"client_to_server","client.99p")
	
		client.99.9p = aggregate( data[,"client_to_server",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.999))})
		client.99.9p = rename.col(client.99.9p,"client_to_server","client.99.9p")
		
		stats = merge(stats, client.mean,all=T)
		stats = merge(stats, client.90p,all=T)
		stats = merge(stats, client.99p,all=T)
		stats = merge(stats, client.99.9p,all=T)
	}
	if ('server_exec' %in% colnames(data)) {
		cat("start server\n")
		server.mean = aggregate(data[,"server_exec",drop=F],by=list(time=floor(data$end/interval)), FUN=mean)
		server.mean = rename.col(server.mean,"server_exec","server.mean")
	
		server.90p = aggregate( data[,"server_exec",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.9))})
		server.90p = rename.col(server.90p,"server_exec","server.90p")
	
		server.99p = aggregate( data[,"server_exec",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.99))})
		server.99p = rename.col(server.99p,"server_exec","server.99p")
	
		server.99.9p = aggregate( data[,"server_exec",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.999))})
		server.99.9p = rename.col(server.99.9p,"server_exec","server.99.9p")
		
		stats = merge(stats, server.mean,all=T)
		stats = merge(stats, server.90p,all=T)
		stats = merge(stats, server.99p,all=T)
		stats = merge(stats, server.99.9p,all=T)
	}
	if ('queue_wait' %in% colnames(data)) {
		cat("start queue\n")
		queue.mean = aggregate(data[,"queue_wait",drop=F],by=list(time=floor(data$end/interval)), FUN=mean)
		queue.mean = rename.col(queue.mean,"queue_wait","queue.mean")
	
		queue.90p = aggregate( data[,"queue_wait",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.9))})
		queue.90p = rename.col(queue.90p,"queue_wait","queue.90p")
	
		queue.99p = aggregate( data[,"queue_wait",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.99))})
		queue.99p = rename.col(queue.99p,"queue_wait","queue.99p")
	
		queue.99.9p = aggregate( data[,"queue_wait",drop=F], by=list(time=floor(data$end/interval)), FUN=function(x){return(quantile(x,0.999))})
		queue.99.9p = rename.col(queue.99.9p,"queue_wait","queue.99.9p")
	
		stats = merge(stats, queue.mean,all=T)
		stats = merge(stats, queue.90p,all=T)
		stats = merge(stats, queue.99p,all=T)
		stats = merge(stats, queue.99.9p,all=T)
	}
	
	types = as.character( unique(data$type) )
	for (type in types) {
		dataT = data[data$type==type,]
		latency.mean = aggregate( dataT[,"latency",drop=F], by=list(time=floor(dataT$end/interval)), FUN=mean)
		latency.mean$latency = latency.mean$latency #/ 1000
		latency.mean = rename.col(latency.mean,"latency",paste(type,".latency.mean",sep=""))
		stats = merge(stats,latency.mean,all=T)

		latency.90p = aggregate( dataT[,"latency",drop=F], by=list(time=floor(dataT$end/interval)), function(x){return(quantile(x,0.9))})
		latency.90p$latency = latency.90p$latency #/ 1000
		latency.90p = rename.col(latency.90p,"latency",paste(type,".latency.90p",sep=""))
		stats = merge(stats,latency.90p,all=T)

		latency.99p = aggregate( dataT[,"latency",drop=F], by=list(time=floor(dataT$end/interval)), function(x){return(quantile(x,0.99))})
		latency.99p$latency = latency.99p$latency #/ 1000
		latency.99p = rename.col(latency.99p,"latency",paste(type,".latency.99p",sep=""))
		stats = merge(stats,latency.99p,all=T)

		latency.99.9p = aggregate( dataT[,"latency",drop=F], by=list(time=floor(dataT$end/interval)), function(x){return(quantile(x,0.999))})
		latency.99.9p$latency = latency.99.9p$latency #/ 1000
		latency.99.9p = rename.col(latency.99.9p,"latency",paste(type,".latency.99.9p",sep=""))
		stats = merge(stats,latency.99.9p,all=T)
	}
	
	return( list(stats=stats,interval=interval,types=types,report_fraction=report_fraction) )
}

plot.stats = function(stats, title) {
	layout( matrix(1:(length(stats$types)+1),byrow=T) )
	plot.workload.vs.throughput(stats, title)
	
	for (type in stats$types)
		plot.type.stats(stats,type,,0.1)	
}

plot.workload.vs.throughput = function(stats, title) {
	xlim = c(0,sort(stats$stats$workload,decreasing=T)[1]*1.10)
	plot( stats$stats$workload, stats$stats$throughput, xlim=xlim,xlab="workload", ylab="throughput", bty="n", main=title )
}

plot.users.vs.workload = function(data,interval,report_fraction=1) {
	nt.seq = sort(unique(data$threads))
	first = nt.seq[1]
	
	dataT = data[data$threads==first,]
	workload = aggregate( dataT[,"start",drop=F], by=list(time=floor(dataT$start/interval)), FUN=length)
	workload = rename.col(workload,"start","workload")
	workload = workload * (1/report_fraction)
	workload = make.col(workload,rep(first,dim(workload)[1]),"threads")
	
	for (nt in nt.seq) {
		dataT = data[data$threads==nt,]
		cat(paste(dim(dataT)[1],"\n"))
		workload_tmp = aggregate( dataT[,"start",drop=F], by=list(time=floor(dataT$start/interval)), FUN=length)
		workload_tmp = rename.col(workload_tmp,"start","workload")
		workload_tmp = workload_tmp * (1/report_fraction)
		workload_tmp = make.col(workload_tmp,rep(nt,dim(workload_tmp)[1]),"threads")
		workload = merge(workload,workload_tmp,all=T)
	}

	boxplot(workload$workload ~workload$threads,xlab="# users (threads)",ylab=paste("workload [req /",interval," [s]"))
	title(paste("Users vs. Workload\naggregation interval: ",interval,"[s]"))	

}

plot.type.stats = function(stats,type,metric="latency",ylim=10) {
	prefix = ""
	if (nchar(type)>0) {prefix=paste(type,".",sep="")}
	col.mean = paste(prefix,metric,".mean",sep="")
	col.90p = paste(prefix,metric,".90p",sep="")
	col.99p = paste(prefix,metric,".99p",sep="")
	col.99.9p = paste(prefix,metric,".99.9p",sep="")
	
	#xlim = c(0,max(stats$stats$throughput)*1.05)
	xlim = c(0,sort(stats$stats$workload,decreasing=T)[1]*1.10)
	
	plot( stats$stats$workload, stats$stats[,col.99p], col="red", ylim=c(0,ylim), xlim=xlim, xlab=paste("workload [#req / ",stats$interval," ms]",sep=""), ylab=paste(metric," [ms]"), bty="n", main=paste("req type: ",type," ",metric,sep=""), ylog="")
	points( stats$stats$workload, stats$stats[,col.99.9p], col="black")
	points( stats$stats$workload, stats$stats[,col.90p], col="blue")
	points( stats$stats$workload, stats$stats[,col.mean], col="green")
	
	legend(x="topleft",legend=c("99.9th perc", "99th perc", "90th perc", "mean"), col=c("black","red","blue","green"), pch=21, inset=0.01)
}


plot.tmp = function(data) {
	nt.seq = sort(unique(data$threads))
	
	for (nt in nt.seq) {
		i = data$threads==nt
		hist( data[i,"server_exec"] )
		start = min( data[i,"start"] )
		end = max( data[i,"end"])
		cat( nt,end-start,"\n")
	}	
}

plot.avg_rps= function(data,report_fraction) {
	threads = sort(unique(data$threads))
	tot_rps = rep(0,length(threads))
	tot_rpt = rep(0,length(threads))
	d<-data.frame(threads,tot_rps,tot_rpt)
	
	for (nt in threads) {
		start = sort(data[data$threads==nt,]$start,decreasing=F)[1]
		end = sort(data[data$threads==nt,]$end,decreasing=T)[1]
		requests = dim(data[data$threads==nt,])[1]
		
		d[d$threads==nt,]$tot_rps= (requests*(1/report_fraction)*nt)/(end-start)
		d[d$threads==nt,]$tot_rpt= (requests*(1/report_fraction))/(end-start)
		cat( nt,(requests*(1/report_fraction)*nt)/(end-start),"\n")
	}
	layout( t(matrix(1:2)) )
	plot(d$threads,d$tot_rps, main="Total requests / Total time\nfor increasing # users",ylab="total requests per sec",xlab="# users [threads]")
		plot(d$threads,d$tot_rpt, main="Total requests per thread/ Total time\nfor increasing # users",ylab="total requests per thread per sec",xlab="# users [threads]")
}

plot.layers.stats = function(stats) {
	layout( matrix(1:3,byrow=T) )
	plot.type.stats(stats,"","client",sort(stats$stats$client.99.9p,decreasing=T)[3]*1.1)
	plot.type.stats(stats,"","server",sort(stats$stats$server.99.9p,decreasing=T)[1]*1.1)
	plot.type.stats(stats,"","queue",sort(stats$stats$server.99.9p,decreasing=T)[1]*1.1)
}
