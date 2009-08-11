load.libraries = function() {
  library(RMySQL)
}

W.FRAC = 0.02
INTERVAL = 20.0
#PAR.DEF = par(no.readonly = TRUE)
PAR.DEF = list( mar=c(5, 4, 4, 4) )

mycols = c("cornflowerblue","navy","royalblue")

action.colors = list("SplitInTwo"="cornflowerblue","MergeTwo"="orangered")

CONNS <<- c()

to.date = function(timestamp) { return( ISOdate(1970,1,1) + timestamp/1000 ) }
print.long.number = function(x) { formatC(round(x),format="f",digits=0) }

plot.scads.performance = function(out.file) {
  load.libraries()
  conn = dbConnect(MySQL(),user="root",password="",dbname="metrics")
  try( plot.scads.performance.raw(conn) )
  dbDisconnect(conn)
}

plot.init = function(out.file=NULL, dbhost="localhost", ts0=NULL, ts1=NULL) {
	load.libraries()  
 	conn.metrics = dbConnect(MySQL(),user="root",password="",dbname="metrics",host=dbhost)
	conn.director = dbConnect(MySQL(),user="root",password="",dbname="director",host=dbhost)

	CONNS <<- c(CONNS,conn.metrics,conn.director)
	print(CONNS)
	
	if (!is.null(ts0) && ts0<0 && is.null(ts1)) {
		## show the last 'ts0' milliseconds
	  	data = get.data(conn.metrics,"ALL","ALL","workload","workload")
	  	ts1 = max(data$time,na.rm=T)
	  	ts0 = max(ts1 + ts0, min(data$time,na.rm=T))
		xtlim = c(ISOdate(1970,1,1)+ts0/1000, ISOdate(1970,1,1)+ts1/1000)
		
	} else if (!is.null(ts0) && !is.null(ts1) && ts0>0 && ts1>ts0) {
		## show data between ts0 and ts1
		xtlim = c(ISOdate(1970,1,1)+ts0/1000, ISOdate(1970,1,1)+ts1/1000)
		
	} else {
		## show all data
	  	data = get.data(conn.metrics,"ALL","ALL","workload","workload")
		data$time = ISOdate(1970,1,1) + data$time/1000
		t0 = min(data$time,na.rm=T)
		t1 = max(data$time,na.rm=T)
		xtlim = c(t0,t1)
	}
		
	print(xtlim)
		
	if (!is.null(out.file))
		png(filename=out.file)
	else if (names( dev.cur() )[1]=="null device")
		quartz()
		
	return( list(conn.metrics=conn.metrics,conn.director=conn.director,xtlim=xtlim) )
}

start.device = function(out.file=NULL,width=500,height=500) {
	if (names( dev.cur() )[1]=="null device") 
		dev.off()
	
	if (!is.null(out.file))
		png(filename=out.file,width=width,height=height)
	else if (names( dev.cur() )[1]=="null device")
		quartz(width=width,height=height)
}

plot.done = function(meta) {
	disconnect.all()
	if (names( dev.cur() )[1]!="quartz")
		dev.off()
}

disconnect.all = function() {
	for (c in CONNS)
		dbDisconnect(c)
	CONNS <<- c()
}

graph.init = function(tight) {
	if (tight) {
		# bottom, left, top, right
		par(mar=c(1, 4, 0, 4))
	} else {
		par(mar=c(3, 4, 2, 4))
	}
}

graph.done = function() {
	par(PAR.DEF)
}

plot.director.simple = function(out.file=NULL,debug=F,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,dbhost,ts0,ts1)

	layout( matrix(1:3, 3, 1, byrow = TRUE), heights=c(3,3,1) )
	plot.all.workload(m,title="SCADS workload")
	plot.all.latency(m,title="SCADS latency")
	plot.actions(m,tight=T)
	
 	plot.done(m)
}

plot.director = function(debug=F,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,dbhost,ts0,ts1)

	all.servers = get.all.servers(m)
	n = length(all.servers)
#	start.device(out.file,500,500+100*n)

	layout( matrix(1:(2*n+3), 2*n+3, 1, byrow = TRUE), heights=c(c(3,3,1),rep(1,2*n)) )
#	layout( matrix(1:4, 4, 1, byrow = TRUE), heights=c(3,3,1,1) )
	plot.all.workload(m,title="SCADS workload")
	plot.all.latency(m,title="SCADS latency")
	plot.actions(m,tight=T)
	
#	plot.server.workload(m,all.servers[1],tight=T,title=all.servers[1])
	for (server in all.servers) {
		plot.server.workload(m,server,tight=T,title=server)
		plot.server.latency(m,server,tight=T,title=F)
	}

	plot.done(m)
}

plot.test = function(debug=T,out.file=NULL,dbhost="localhost",ts0=NULL,ts1=NULL) {
	m = plot.init(out.file,dbhost,ts0,ts1)
	plot.all.workload(m,debug=T,title="SCADS workload")
	plot.all.latency(m,"SCADS latency")
	plot.actions(m,tight=T)
	plot.done(m)
}

plot.all.workload = function(m,debug=T,tight=F,title="") {
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

plot.server.workload = function(m,server,tight=T,title=T) {
	data = get.data(m$conn.metrics,server,"ALL","workload","all_workload")
	data$time = to.date(data$time)
	
	graph.init(tight=T)
	plot( data$time, data$all_workload, xlim=m$xtlim, ylim=c(0,max(data$all_workload)), bty="n", axes=F, xlab="", ylab="workload", type="l", main=title )
	graph.done()
}

plot.server.latency = function(m,server,tight=T,title=T) {
	data = 				get.data(m$conn.metrics,server,"get","latency_90p","get_latency_90p")
	data = merge( data, get.data(m$conn.metrics,server,"put","latency_90p","put_latency_90p") )
	data$time = to.date(data$time)
	
	graph.init(tight=T)
	plot( c(), xlim=m$xtlim, ylim=c(0,100), bty="n", axes=F, xlab="", ylab="latency [ms]")
	lines( data$time, data$get_latency_90p, col="red" )
	lines( data$time, data$put_latency_90p, col="blue" )
	graph.done()
}

plot.all.latency = function(m,server,tight=F,title=F) {
	graph.init(tight)
	data = 				get.data(m$conn.metrics,"ALL","get","latency_mean","get_latency_mean")
	data = merge( data, get.data(m$conn.metrics,"ALL","get","latency_90p","get_latency_90p") )
#	data = merge( data, get.data(m$conn.metrics,"ALL","get","latency_99p","get_latency_99p") )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_mean","put_latency_mean") )
	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_90p","put_latency_90p") )
#	data = merge( data, get.data(m$conn.metrics,"ALL","put","latency_99p","put_latency_99p") )
	data$time = to.date(data$time)

	d = unlist( data[,setdiff(names(data),c("time","timestamp"))] )
	ylim = c(0, quantile(d,0.9,na.rm=T))

	plot( c(), xlim=m$xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab="latency [ms]", main=title)
	axis.POSIXct(1,x=m$xtlim,format="%H:%M")
	axis(2)
	lines( data$time, data$get_latency_mean, col="blue", lty=3 )
	lines( data$time, data$get_latency_90p, col="blue", lty=1 )
#	lines( data$time, data$get_latency_99p, col="blue", lty=3 )
	lines( data$time, data$put_latency_mean, col="red", lty=3 )
	lines( data$time, data$put_latency_90p, col="red", lty=1 )
#	lines( data$time, data$put_latency_99p, col="red", lty=3 )
	legend( "bottomleft", legend=c("get mean","get 90p","put mean","put 90p"), col=c("blue","blue","red","red"), inset=0.02, lty=c(3,1,3,1), lwd=2 )
	graph.done()
}


plot.policy.state = function(conn.director, xtlim) {
	state = dbGetQuery(conn.director, "select * from policystate")
	state$time = ISOdate(1970,1,1) + state$time/1000
	
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
	actions$update_timeD = ISOdate(1970,1,1) + actions$update_time/1000
	actions$init_timeD = ISOdate(1970,1,1) + actions$init_time/1000
	actions$start_timeD = ISOdate(1970,1,1) + actions$start_time/1000
	actions$end_timeD = ISOdate(1970,1,1) + actions$end_time/1000

	plot( c(), xlim=m$xtlim, ylim=c(-1,1), axes=F, ylab="", xlab="" )
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

get.all.servers = function(m) {
  return( dbGetQuery(m$conn.metrics, "select distinct server from scads_metrics")$server )
}

get.data = function(c,server,req.type,stat,col.name) {
	sql = paste("select time as timestamp, value as ",col.name," from scads where metric_id in (select id from scads_metrics where server='",server,"' and request_type='",req.type,"' and stat='",stat,"') ",sep="")
  d = dbGetQuery(c, sql )
  d[,col.name] = as.numeric(d[,col.name])
  return(d)
}

