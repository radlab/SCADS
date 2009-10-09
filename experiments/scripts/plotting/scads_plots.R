load.libraries = function() {
  library(RMySQL)
}

W.FRAC = 0.02
INTERVAL = 20.0
#PAR.DEF = par(no.readonly = TRUE)
PAR.DEF = list( mar=c(5, 4, 4, 4) )

DBUSER = "root"
DBPASS = ""

AGG = 20000

mycols = c("cornflowerblue","navy","royalblue")

action.colors = list("SplitInTwo"="cornflowerblue",
					"MergeTwo"="orangered",
					"ReplicateFrom"="blue",
					"RemoveFrom"="green",
					"Remove"="cornflowerblue")

CONNS <<- c()

to.date = function(timestamp) { return( c(ISOdate(1970,1,1) + as.numeric(timestamp)/1000 )) }
print.long.number = function(x) { formatC(round(x),format="f",digits=0) }

plot.scads.performance = function(out.file) {
  load.libraries()
  conn = dbConnect(MySQL(),user=DBUSER,password=DBPASS,dbname="metrics")
  try( plot.scads.performance.raw(conn) )
  dbDisconnect(conn)
}

plot.init = function(out.file=NULL, plot.width=NULL, plot.height=NULL, dbhost="localhost", ts0=NULL, ts1=NULL) {
	load.libraries()  
 	conn.metrics = dbConnect(MySQL(),user=DBUSER,password=DBPASS,dbname="metrics",host=dbhost)
	conn.director = dbConnect(MySQL(),user=DBUSER,password=DBPASS,dbname="director",host=dbhost)

	CONNS <<- c(CONNS,conn.metrics,conn.director)
	print(CONNS)
	
	if (!is.null(ts0) && ts0<0 && is.null(ts1)) {
		## show the last 'ts0' milliseconds
	  	data = get.data(conn.metrics,"ALL","ALL","workload","workload")
	  	ts1 = max(data$time,na.rm=T)
	  	ts0 = max(ts1 + ts0, min(data$time,na.rm=T))
		xtlim = c( to.date(ts0), to.date(ts1) )
		
	} else if (!is.null(ts0) && !is.null(ts1) && ts0>0 && ts1>ts0) {
		## show data between ts0 and ts1
		xtlim = c( to.date(ts0), to.date(ts1) )
		
	} else {
		## show all data
	  	data = get.data(conn.metrics,"ALL","ALL","workload","workload")
		t0 = to.date( min(data$time,na.rm=T) )
		t1 = to.date( max(data$time,na.rm=T) )
		xtlim = c(t0,t1)		
	}
	
	start.device(out.file,plot.width,plot.height)
#	if (!is.null(out.file))
#		png(filename=out.file, width=plot.width, height=plot.height)
#	else if (names( dev.cur() )[1]=="null device")
#		quartz()
		
	return( list(conn.metrics=conn.metrics,conn.director=conn.director,xtlim=xtlim) )
}

start.device = function(out.file=NULL,width=500,height=500) {
	if (names( dev.cur() )[1]=="null device" || names( dev.cur() )[1]=="quartz") 
		try( dev.off() )
	
	N_SCREEN <<- 1
		
	if (!is.null(width) && !is.null(height)) {
		if (!is.null(out.file)) {
			if ( length(grep("pdf$",out.file))>0 ) pdf(file=out.file,width=width/70,height=height/70)
			else png(filename=out.file,width=width,height=height)
		} else {
			quartz(width=width/70,height=height/70)
		}
	}
}

plot.done = function(meta,close.db=T) {
	close.screen(all = TRUE)
	if (close.db) disconnect.all()
	if (names( dev.cur() )[1]!="quartz")
		dev.off()
}

disconnect.all = function() {
	for (c in CONNS)
		dbDisconnect(c)
	CONNS <<- c()
}

graph.init = function(tight=F, tight.top=0, tight.bottom=0) {
	screen(N_SCREEN)
	N_SCREEN <<- N_SCREEN+1

	# set margins of the plot
	mar.left = 4
	mar.right = 4
	if (tight) { tight.top=1; tight.bottom=2 }
	if (tight.bottom==0) 		mar.bottom = 3
	else if (tight.bottom==1) 	mar.bottom = 1
	else if (tight.bottom==2) 	mar.bottom = 0.2
	if (tight.top==0) 		mar.top = 2
	else if (tight.top==1) 	mar.top = 1
	else if (tight.top==2) 	mar.top = 0.2
	par(mar=c(mar.bottom,mar.left,mar.top,mar.right))
	
	# set distance of axis title, axis label, and axis line
	par(mgp=c(2,0.6,0))
	#par(cex=0.7)
}

graph.done = function() {
	par(PAR.DEF)
}

screen.coord = function(ysizes) {
	n = length(ysizes)
	s = sum(ysizes)
	matrix(c( rep(0,n), rep(1,n), 1-(cumsum(ysizes)/s), 1-(cumsum(c(0,ysizes[-n]))/s) ), nrow=n )
}

plot.director.simple = function(out.file=NULL,debug=F,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,600,650,dbhost,ts0,ts1)
	try( plot.director.simple.raw(m,out.file,debug,dbhost,ts0,ts1) )
 	plot.done(m)
}

plot.director.simple.raw = function(m,out.file=NULL,debug=F,dbhost="localhost",ts0=NULL,ts1=NULL) {
	split.screen( screen.coord(c(3,2,2,2,2)) )
	try( plot.workload(m,title="workload") )
	try( plot.workload.mix(m,title="workload mix",tight=T) )
	try( plot.all.latency(m,title="latency",tight=T) )
	try( graph.nservers.from.configs(m,tight=T) )
	try( plot.actions(m,tight=T) )
}

plot.director = function(debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,NULL,NULL,dbhost,ts0,ts1)
	try( plot.director.raw(m,debug,out.file,dbhost,ts0,ts1) )
	plot.done(m)
}

plot.director.raw = function(m,debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	all.servers = get.all.servers(m)
	n = length(all.servers)
	start.device(out.file,600,650+30*n)

#	layout( matrix(1:(2*n+4), 2*n+4, 1, byrow = TRUE), heights=c(c(3,2,3,1),1.5,rep(0.5,n-1),1.5,rep(0.5,n-1)) )
#	sc = screen.coord(c(c(3,2,3,3,1),1.5,rep(0.5,n-1),1.5,rep(0.5,n-1)))
	sc = screen.coord(c(c(3,2,3,3,1),rep(0.5,n-1)))
	split.screen( sc )

	plot.workload(m,title="workload")
	plot.workload.mix(m,title="workload mix")
	plot.all.latency(m,title="latency")
	graph.nservers.from.configs(m)
	plot.actions(m,tight=T)
	
	max.sw = get.max.server.workload(m$conn.metrics)
	
#	plot.server.workload(m,all.servers[1],tight=T,title=all.servers[1])
	for (server in all.servers) 
		if (server=="ALL") cat("skipping ALL\n") #plot.server.workload(m,server,tight=F,title="workload")
		else plot.server.workload(m,server,tight=T,title="",max.sw=max.sw)
		
#	for (server in all.servers) 
#		if (server=="ALL") plot.server.latency(m,server,tight=F,title="latency")
#		else plot.server.latency(m,server,tight=T,title="")
}

plot.test = function(debug=T,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,dbhost,ts0,ts1)
	plot.all.workload(m,debug=T,title="SCADS workload")
	plot.all.latency(m,"SCADS latency")
	plot.actions(m,tight=T)
	plot.done(m)
}

plot.all.workload = function(m,debug=F,tight=F,title="") {
	graph.init(tight)
	gets = get.data(m$conn.metrics,"ALL","get","workload","getw")
	puts = get.data(m$conn.metrics,"ALL","put","workload","putw")
	all = get.data(m$conn.metrics,"ALL","ALL","workload","allw")
	data = merge( all, merge(gets,puts) )
	if (debug) { print( c(print.long.number(min(data$time,na.rm=T)), print.long.number(max(data$time,na.rm=T))) ) }
	data$time = to.date(data$time)
	
	ylim = c(0, max(data[,c("getw","putw","allw")],na.rm=T))
	
	plot( data$time, data$allw, xlim=m$xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab="workload", type="l", main=title )
	axis.POSIXct(1,x=m$xtlim,format="%H:%M")
	axis(2)
	lines( data$time, data$getw, col="blue" )
	lines( data$time, data$putw, col="red" )
	legend( "topleft", legend=c("all","get","put"), col=c("black","blue","red"), inset=0.02, lty=c(1,1,1), lwd=2 )
	graph.done()
}

get.workload.data = function(m) {
	gets = get.data(m$conn.metrics,"ALL","get","workload","getw")
	puts = get.data(m$conn.metrics,"ALL","put","workload","putw")
	all = get.data(m$conn.metrics,"ALL","ALL","workload","allw")
	data = merge( all, merge(gets,puts) )
	data$time = to.date(data$time)
	data
}

plot.workload = function(m,debug=F,tight=F,title="",data=NA,vline=NA) {
	graph.init(tight.bottom=0,tight.top=1)
	if (is.na(data))
		data = get.workload.data(m)
	ylim = c(0, max(data[,c("getw","putw","allw")],na.rm=T))
	
	plot( data$time, data$allw, xlim=m$xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab="request rate", type="l", main=title, cex.lab=1.4 )
	axis.POSIXct(1,x=m$xtlim,format="%H:%M")
	axis(2)
	if (!is.na(vline))
		abline(v=to.date(as.numeric(vline)),col="olivedrab",lwd=2)
	#lines( data$time, data$getw, col="blue" )
	#lines( data$time, data$putw, col="red" )
	#legend( "topleft", legend=c("all","get","put"), col=c("black","blue","red"), inset=0.02, lty=c(1,1,1), lwd=2 )
	graph.done()
}

get.workload.mix.data = function(m) {
	gets = get.data(m$conn.metrics,"ALL","get","workload","getw")
	puts = get.data(m$conn.metrics,"ALL","put","workload","putw")
	all = get.data(m$conn.metrics,"ALL","ALL","workload","allw")
	data = merge( all, merge(gets,puts) )
	data$time = to.date(data$time)
	data
}

plot.workload.mix = function(m,debug=F,tight=F,title="",data=NA,vline=NA) {
	graph.init(tight.bottom=2,tight.top=1)
	if (is.na(data))
		data = get.workload.mix.data(m)
	
	plot( data$time, data$getw/data$allw, c(), xlim=m$xtlim, ylim=c(0.005,1), bty="n", axes=F, xlab="", ylab="mix [log]", type="l", main=title, log="y", cex.lab=1.4 )
#	plot( c(), c(), xlim=m$xtlim, ylim=c(0,1), bty="n", axes=F, xlab="", ylab="workload", type="l", main=title )
	#axis.POSIXct(1,x=m$xtlim,format="%H:%M")
	axis(2, c(0.01,0.05,1.0))
	if (!is.na(vline))
		abline(v=to.date(as.numeric(vline)),col="olivedrab",lwd=2)
	lines( data$time, data$getw/data$allw, col="blue" )
	lines( data$time, data$putw/data$allw, col="red" )
	legend( "topleft", legend=c("get","put"), col=c("blue","red"), inset=0.02, lty=c(1,1), lwd=2 )
	graph.done()
}

plot.server.workload = function(m,server,tight=T,title=T,max.sw) {
	data = get.data(m$conn.metrics,server,"ALL","workload","all_workload")
	data$time = to.date(data$time)
	
	graph.init(tight=tight)
	if (missing(max.sw))
		ylim = c(0,round(max(data$all_workload)))
	else
		ylim = c(0,max.sw)
	plot( data$time, data$all_workload, xlim=m$xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab=server, type="l", main=title )
	abline( h=ylim[2]/2, col="gray", lty=3 )
	if (!missing(max.sw))axis(2,ylim) else axis(2)
	
	tmin = data$time[ which.min( data$all_workload ) ]
	wmin = min(data$all_workload,na.rm=T)
	tmax = data$time[ which.max( data$all_workload ) ]
	wmax = max(data$all_workload,na.rm=T)
	xmax = max( data$time, na.rm=T )
	wlast = data$all_workload[ length(data$all_workload) ]
	points(x=c(tmin,tmax),y=c(wmin,wmax),pch=19,col=c("blue","red"),cex=0.5)
	text(x=xmax,y=ylim[2]*0.15,labels=signif(wmin,3),pos=4,col="blue", xpd=T) 
	text(x=xmax,y=ylim[2]*0.85,labels=signif(wmax,3),pos=4,col="red", xpd=T)
	text(x=xmax,y=ylim[2]*0.5,labels=signif(wlast,3),pos=4, xpd=T)
	
	graph.done()
}

plot.server.latency = function(m,server,tight=T,title=T) {
	data = 				get.data(m$conn.metrics,server,"get","latency_90p","get_latency_90p",replace.inf=1000)
	data = merge( data, get.data(m$conn.metrics,server,"put","latency_90p","put_latency_90p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,server,"get","latency_99p","get_latency_99p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,server,"put","latency_99p","put_latency_99p",replace.inf=1000) )
	data$time = to.date(data$time)
	
	graph.init(tight=tight)
	plot( c(), xlim=m$xtlim, ylim=c(0,150), bty="n", axes=F, xlab="", ylab=server, main=title)
	axis(2,c(0,150))
#	lines( data$time, data$get_latency_90p, col="blue" )
#	lines( data$time, data$put_latency_90p, col="red" )
	lines( data$time, data$get_latency_99p, col="blue" )
	lines( data$time, data$put_latency_99p, col="red" )
	graph.done()
}

get.all.latency.data = function(m) {
	data = 				get.data(m$conn.metrics,"ALL","get","latency_mean","get_latency_mean",replace.inf=1000)
	data = merge( data, get.data(m$conn.metrics,"ALL","get","latency_50p","get_latency_50p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","get","latency_90p","get_latency_90p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","get","latency_99p","get_latency_99p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_mean","put_latency_mean",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_50p","put_latency_50p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_90p","put_latency_90p",replace.inf=1000) )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_99p","put_latency_99p",replace.inf=1000) )
	data$time = to.date(data$time)
	data	
}

plot.all.latency = function(m,server,tight=F,title=F,plot.mean=T,data=NA,vline=NA) {
	graph.init(tight.top=1,tight.bottom=2)
	if (is.na(data))
		data = get.all.latency.data(m)

	d = unlist( data[,setdiff(names(data),c("time","timestamp"))] )
#	ylim = c(0, quantile(d,0.9,na.rm=T))
	ylim = c(0, 150)

	plot( c(), xlim=m$xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab="latency [ms]", main=title)
	#axis.POSIXct(1,x=m$xtlim,format="%H:%M")
	#axis.POSIXct(1,x=m$xtlim,format=" ")
	axis(2)
#	lines( data$time, data$get_latency_90p, col="blue", lty=1 )
	lines( data$time, data$get_latency_99p, col="cornflowerblue", lty=1 )
	lines( data$time, data$put_latency_99p, col="orange", lty=1 )
	if (plot.mean) {
		lines( data$time, data$get_latency_50p, col="cornflowerblue", lty=3 )
		lines( data$time, data$put_latency_50p, col="orange", lty=3 )
	}
#	lines( data$time, data$put_latency_90p, col="red", lty=1 )
	if (!is.na(vline))
		abline(v=to.date(as.numeric(vline)),col="olivedrab",lwd=2)
	if (!plot.mean)
		legend( "topleft", legend=c("get 99p","put 99p"), col=c("cornflowerblue","orange"), inset=0.02, lty=c(3,1,3,1), lwd=2 )
	else 
		legend( "topleft", legend=c("get median","get 99p","put median","put 99p"), col=c("cornflowerblue","cornflowerblue","orange","orange"), inset=0.02, lty=c(3,1,3,1), lwd=2 )
	graph.done()
}


plot.policy.state = function(conn.director, xtlim) {
	state = dbGetQuery(conn.director, "select * from policystate")
	state$time = to.date(state$time)
	
	states = unique(state$state)
	n = length(states)
	colors = rainbow(n)
	plot( c(), xlim=xtlim, ylim=c(0,1), axes=F, bty="n", xlab="" )
	axis.POSIXct(1, x=xtlim, format="%H:%M")
	for (i in 1:n) {
		j = state$state==states[i]
		points( state[j,"time"], rep(i/n,sum(j)), col=colors[i] )
	}
}

plot.actions = function(m,tight=F,debug=F) {
	graph.init(tight)
	actions = dbGetQuery(m$conn.director, "select * from actions" )
	actions$update_timeD = to.date(actions$update_time)
	actions$init_timeD = to.date(actions$init_time)
	actions$start_timeD = to.date(actions$start_time)
	actions$end_timeD = to.date(actions$end_time)

	plot( c(), xlim=m$xtlim, ylim=c(-0.5,1), axes=F, ylab="", xlab="" )
	if (!tight) axis.POSIXct( 1, x=m$xtlim, format="%H:%M")
	for (i in 1:nrow(actions)) {
		y = runif(1,0,1)
		x = c(actions[i,"start_timeD"], actions[i,"end_timeD"])
		lines( x, c(y,y), col=action.colors[[actions[i,"action_name"]]], lwd=2 )
	}
	
	cols = c()
	names = unique(actions$action_name)
	for (type in names) cols = c(cols,action.colors[[type]])
	legend(x="bottomleft",legend=names,col=cols,lwd=2,horiz=T)
	
	graph.done()
}

plot.scads.performance.raw = function(conn) {
  all.servers = get.all.servers(conn)
  nrow = length(all.servers)

  png(file=out.file,width=2*500,height=nrow*400)
  layout( matrix(1:(nrow*2), nrow, 2, byrow = TRUE) )

  plot.legend = T

  for (server in all.servers) {
    for (req.type in c("get","put")) {
      data = get.data(conn,server,req.type,"workload","workload")
      data = merge( data, get.data(conn,server,req.type,"latency_mean","latency_mean") )
      data = merge( data, get.data(conn,server,req.type,"latency_90p","latency_90p") )
      data = merge( data, get.data(conn,server,req.type,"latency_99p","latency_99p") )

      try( plot.single.reqtype(data,title=paste(req.type,"@",server),plot.legend=plot.legend) )
      plot.legend = F
    }
  }
  dev.off()
}

plot.empty = function() {
  plot( c(), c(), axes=F, xlab="", ylab="", xlim=c(0,1), ylim=c(0,1) )
}

plot.single.reqtype = function(data,title,plot.legend=F) {
  par(mar=c(5, 4, 4, 6))

  workload = data$workload/W.FRAC/INTERVAL
  max.w = max( workload, na.rm=T )
  data$time = ISOdate(1970,1,1) + data$timestamp/1000
  plot( data$time, workload, bty="n", axes=F, xlab="time", ylab="workload [req/s]", type="p", lwd=2, main=title, ylim=c(0,max.w) )
  axis.POSIXct( 1, x=data$time, format="%H:%M")
  axis(2)

  min.l = min( c(data$latency_99p,data$latency_90p,data$latency_mean), na.rm=T )
  max.l = max( c(data$latency_99p,data$latency_90p,data$latency_mean), na.rm=T )

  par(new=TRUE)
  plot( data$time, data$latency_99p, bty="n", axes=F, type="l", col="midnightblue", lwd=2, ylab="", ylim=c(0,max.l) )
  lines( data$time, data$latency_90p, col="royalblue3", lwd=2 )
  lines( data$time, data$latency_mean, col="cornflowerblue", lwd=2 )
  mtext( "latency [msec]", side=4, line=3 )
  axis(4)

  if (plot.legend)
    legend( "topleft", legend=c("workload","latency mean","latency 90p","latency 99p"), inset=0.02,
          col=c("black","cornflowerblue","royalblue3","midnightblue"),
          lty=c(NA,1,1,1), lwd=c(NA,2,2,2), pch=c(21,NA,NA,NA) )
}



#####
# plot of the SCADS state 
#
plot.scads.state.old = function(time0,time1,latency90p.thr=100,out.file=NULL,debug=F,dbhost="localhost") {
	m = plot.init(out.file,800,800,dbhost,NULL,NULL)

	max.w = get.max.server.workload(m,time0,time1)
	print(max.w)

	layout( matrix(1:4, 4, 1, byrow = TRUE), heights=c(3,2,3,3) )
	graph.workload.histogram(m,time1,"getRate","darkgreen",title="get workload histogram")
	graph.workload.histogram(m,time1,"putRate","darkred",title="put workload histogram")
	graph.config(m,time0,max.w,lat.thr=latency90p.thr)
	graph.config(m,time1,max.w,lat.thr=latency90p.thr)
 	plot.done(m)
}

plot.all.scads.states = function(dir,dbhost="localhost",n.skip=0) {
	m = plot.init(out.file,NULL,NULL,dbhost,NULL,NULL)
	all.times = get.times.of.all.configs(m$conn.director)
	times = all.times[ seq(1,length(all.times),by=n.skip+1) ]
	
	workload.data = get.workload.data(m)
	workload.mix.data = get.workload.mix.data(m)
		
	for (time in times) {
		start.device(paste(dir,"/state_",print.long.number(time),".png",sep=""),800,600)
		try( plot.scads.state.raw(m,ts0=time,workload.data=workload.data,workload.mix.data=workload.mix.data) )
		plot.done(close.db=F)
	}
	disconnect.all()
}

plot.scads.state = function(debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,800,600,dbhost,ts0,ts1)
	try( plot.scads.state.raw(m,ts0) )
	plot.done(m)	
}

plot.scads.state.raw = function(m,ts0=NULL,workload.data=NA,workload.mix.data=NA,latency.data=NA) {
	ss = split.screen( screen.coord(c(3,2,4,2)) )
	servers = setdiff( get.all.servers(m), "ALL" )
	plot.workload(m,vline=ts0,data=workload.data)
	plot.workload.mix(m,title="workload mix",tight=T,vline=ts0,data=workload.mix.data)
	#plot.all.latency(m,"ALL",title="performance",plot.mean=F,vline=ts0,data=latency.data)
	graph.state.servers(m,servers,ts0,title=paste("state at ",to.date(ts0),sep=""))
	graph.state.servers.nkeys(m,servers,ts0)	
}

plot.configs = function(debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,NULL,NULL,dbhost,ts0,ts1)
	try( plot.configs.raw(m,debug,out.file,dbhost,ts0,ts1))
	plot.done(m)
}

plot.configs.raw = function(m,debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	configs = get.changed.configs(m$conn.director)
	max.w = get.max.server.workload(m$conn.metrics)

	start.device(out.file,1000,60*length(configs))
	split.screen(c(length(configs),1))
	for (time in names(configs)) graph.config(m,as.numeric(time),max.w,100,tight=T)
}

graph.state.servers = function(m,servers,time,title) {
	graph.init(tight.top=0,tight.bottom=2)
	max.w = 100.0
	gets = get.stat.for.servers(m$conn.metrics,servers,time,"get","workload",add.missing=T)/3500
	puts = get.stat.for.servers(m$conn.metrics,servers,time,"put","workload",add.missing=T)/150
	k = puts/gets
	height = gets*(k+1)*100
	
	cfg = get.config(m$conn.director,time)
	agg = aggregate( cfg$minKey, by=list(minKey=cfg$minKey), length)
	servers.o = data.frame( server=servers, order=1:length(servers) )
	servers.o = merge( merge(cfg,agg), servers.o, all=T )
	servers.o = servers.o[ order(servers.o$order), ]
	#print(servers.o)
	
	colors = rep( "cornflowerblue", length(servers) )
	colors[servers.o$x>1] = "olivedrab2"
		
	#height = get.stat.for.servers(m$conn.metrics,servers,time,"ALL","workload",add.missing=T)
	barplot( height=height$value, space=0.1, ylim=c(0,max.w), col=colors, ylab="utilization [%]", cex.lab=1.4, axes=F, main=title )
	abline(h=67,lty=2,col="red", lwd=3)
	axis(2,c(0,50,100))
	#, names.arg=servers
	graph.done()
}

graph.state.servers.nkeys = function(m,servers,time) {
	graph.init(tight.top=1,tight.bottom=1)
	cfg = get.config(m$conn.director,time)
	height = data.frame( server=servers, order=1:length(servers) )
	height = merge(height, data.frame( server=cfg$server, nkeys=cfg$maxKey-cfg$minKey ), all=T)
	barplot( height=height$nkeys, space=0.1, ylim=c(3000,0), col="black", ylab="# keys", axes=T, cex.lab=1.4)
	#axis(2,c(0,2000),cex.lab=2,cex.axis=2)
	graph.done()
}

graph.nservers.from.configs = function(m,tight=F) {
	graph.init(tight=tight)

	configs = get.all.configs(m$conn.director)
	data = data.frame()
	for (i in 1:nrow(configs)) {
		date = to.date(as.numeric(configs$time[i]))
		nservers = nrow(read.csv( textConnection(configs$config[i]) ))
		data = rbind(data, data.frame(time=date,nservers=nservers) )
	}
	plot( data$time, data$nservers, bty="n", axes=F, xlab="time", ylab="nservers", lwd=2, main="number of servers", type="l", ylim=c(0,max(data$nservers)))
	#axis.POSIXct( 1, x=data$time, format="%H:%M")
	axis.POSIXct( 1, x=data$time, format=" ")
	axis(2)

	graph.done()
}


graph.workload.histogram = function(m,time,colname,color,tight=T,title=T) {
	data = get.workload.histogram(m$conn.director,time)
	graph.init(tight=F)
	barplot( height=data[,colname], width=data$maxKey-data$minKey, space=0, col=color, xlim=c(min(data$minKey),max(data$maxKey)), main=title )
#	barplot( height=data[,"putRate"], width=data$maxKey-data$minKey, space=0, col="#ff000022", xlim=c(min(data$minKey),max(data$maxKey)), add=T, axes=F )
	graph.done()
}

get.max.server.workload = function(m,time0,time1) {
	servers0 = get.config(m$conn.director,time0)$server
	servers1 = get.config(m$conn.director,time1)$server
	
	w0 = as.numeric( get.stat.for.servers( m$conn.metrics, servers0, time0, "ALL", "workload" )$value )
	w1 = as.numeric( get.stat.for.servers( m$conn.metrics, servers1, time1, "ALL", "workload" )$value )
	
	return( max(c(w0,w1)) )
}

graph.config = function(m,time,max.w,lat.thr=100,tight=F,title=T) {
	data = get.config(m$conn.director,time)
	
	workload = get.stat.for.servers( m$conn.metrics, data$server, time, "ALL", "workload" )$value
	d.get.l = get.stat.for.servers( m$conn.metrics, data$server, time, "get", "latency_90p" )$value
	d.put.l = get.stat.for.servers( m$conn.metrics, data$server, time, "put", "latency_90p" )$value
	
#	print( data )
#	cat("w=",workload,"\n")
#	cat("get.l=",d.get.l,"\n")
#	cat("put.l=",d.put.l,"\n")

	data$range = paste(data[,"minKey"],data[,"maxKey"],sep="-")
	agg = aggregate( data$minKey, by=list(minKey=data$minKey), length)
	#print(data)
	#print(agg)
	bp.matrix = matrix( rep(NA, nrow(agg)*max(agg$x) ), ncol=nrow(agg) )
	width = c()
	colors = c()
	server.i = 1
	for (col.i in 1:nrow(agg)) {
		width = c(width,data$maxKey[server.i]-data$minKey[server.i])
		if (agg$x[col.i]>1) colors = c(colors,"olivedrab2")
		else colors = c(colors,"cornflowerblue")
		for (row.i in 1:agg$x[col.i]) {
			bp.matrix[row.i,col.i] = workload[server.i]
			server.i = server.i+1		
		}
	}	
	#print(width)
	#print(bp.matrix)
	#print(colors)
	
	#colors = rep("cornflowerblue",nrow(data))
	#colors[ as.numeric(d.get.l)>lat.thr ] = "orangered"
	#colors[ as.numeric(d.put.l)>lat.thr ] = "orangered"
	#colors[ is.infinite(d.get.l) ] = "red3"
	#colors[ is.infinite(d.put.l) ] = "red3"
	
	graph.init(tight=tight)
#	barplot( height=workload, width=data$maxKey-data$minKey, space=0, col=colors, xlim=c(min(data$minKey),max(data$maxKey)), axes=T, main=paste("config at time ",to.date(time),sep=""), ylim=c(0,max.w) )
	barplot( height=colSums(bp.matrix,na.rm=T), width=width, space=0, col=colors, border=NA, xlim=c(min(data$minKey),max(data$maxKey)), axes=F )
	barplot( height=bp.matrix, width=width, space=0, col=NA, xlim=c(min(data$minKey),max(data$maxKey)), axes=T, main=paste("config at time ",to.date(time),sep=""), add=T )
	graph.done()
}

get.all.servers = function(m) {
  return( dbGetQuery(m$conn.metrics, "select distinct server from scads_metrics")$server )
}

get.data = function(c,server,req.type,stat,col.name,replace.inf=F) {
	sql = paste("select time as timestamp, value as ",col.name," from scads where metric_id in (select id from scads_metrics where server='",server,"' and request_type='",req.type,"' and aggregation='",AGG,"' and stat='",stat,"') ",sep="")
	d = dbGetQuery(c, sql )

	# fix infinities
	v = d[,col.name]
	v[grep("Infinity",v)] = "Inf"
	v[grep("-Infinity",v)] = "-Inf"
	v = as.numeric(v)
	if (replace.inf) { v[ is.infinite(v) ] = replace.inf }
	d[,col.name] = v
	
	# fix missing timestamps
	m = setdiff( seq(min(d$timestamp),max(d$timestamp),by=min(diff(d$timestamp))), d$timestamp )
	if (length(m)>0) {
		dn = data.frame( t=m, v=NaN )
		colnames(dn) = c("timestamp",col.name)
		d = rbind(d, dn )
		d = d[order(d$timestamp),]
	}
	
  return(d)
}

get.stat.for.servers = function(c,servers,time,req.type,stat,add.missing=F) {
	server.s = paste( "\"", paste(servers,collapse="\",\"", sep=""), "\"", sep="" )
	sql = paste( "select * from metrics.scads s, metrics.scads_metrics m where s.time=",time," and s.metric_id=m.id and m.server IN (",server.s,") and m.request_type=\"",req.type,"\" and m.aggregation=\"",AGG,"\" and m.stat=\"",stat,"\" ", sep="")
	d = dbGetQuery( c, sql )

	# replace Infinity -> Inf
	v = d$value
	v[grep("Infinity",v)] = "Inf"
	v[grep("-Infinity",v)] = "-Inf"
	d$value = v

	#cat("\n** getting data for ",paste(servers,sep=",",collapse=",")," @ ",time," type=",req.type," stat=",stat,"\n",sep="")
	#print(d)
	
	s1 = data.frame( server=servers, order=1:length(servers))
	s2 = data.frame( server=d$server, value=as.numeric(d$value) )
	s = merge(s1,s2)
	ds = s[order(s$order),]
	
	if (add.missing) {
		miss = data.frame( server=servers, order=1:length(servers), value=NA )
		miss = miss[ setdiff((1:length(servers)),ds$order), ]
		#print(miss)
		#print(ds)
		ds = rbind(ds,miss)
		ds = ds[order(ds$order),]
	}
	
	ds
}

get.max.server.workload = function(c) {
	sql = paste( "SELECT MAX(CAST(value as SIGNED)) as max FROM scads_metrics sm, scads s where server!=\"ALL\" and request_type=\"ALL\" and aggregation='",AGG,"' and stat=\"workload\" and sm.id=s.metric_id", sep="" )
	d = dbGetQuery( c, sql )
	return( d[1,1] )
}

get.workload.histogram = function(c,time) {
	sql = paste( "SELECT histogram FROM director.scadsstate_histogram where time='",time,"' ", sep="")
	d = dbGetQuery( c, sql )
	return( read.csv( textConnection(d$histogram[1]) ) )
}

get.config = function(c,time) {
	sql = paste( "SELECT config FROM director.scadsstate_config where time='",time,"' ", sep="")
	d = dbGetQuery( c, sql )
	return( read.csv( textConnection(d$config[1]) ) )
}

get.times.of.all.configs = function(c) {
	sql = "SELECT time FROM director.scadsstate_config s"
	d = dbGetQuery( c, sql )
	return( d$time )
}

get.changed.configs = function(c) {
	sql = paste( "SELECT time, config FROM director.scadsstate_config")
	d = dbGetQuery( c, sql )
	
	i = grep(FALSE,c("",d$config[-nrow(d)]) == d$config)
	cs = list()
	for (ii in i) cs[[as.character(d[ii,"time"])]] = read.csv( textConnection(d$config[ii]) )

	return( cs )
}

get.all.configs = function(c) {
	sql = paste( "SELECT time, config FROM director.scadsstate_config")
	d = dbGetQuery( c, sql )
}