load.libraries = function() {
  library(RMySQL)
}

W.FRAC = 0.02
INTERVAL = 20.0
PAR.DEF = par(no.readonly = TRUE)

to.date = function(timestamp) {
	return( ISOdate(1970,1,1) + timestamp/1000 )
}

plot.scads.performance = function(out.file) {
  load.libraries()
  conn = dbConnect(MySQL(),user="root",password="",dbname="metrics")
  try( plot.scads.performance.raw(conn) )
  dbDisconnect(conn)
}

plot.director = function(out.file,dbhost="localhost") {
  load.libraries()
  
  conn.metrics = dbConnect(MySQL(),user="root",password="",dbname="metrics",host=dbhost)
  conn.director = dbConnect(MySQL(),user="root",password="",dbname="director",host=dbhost)
  
  actions = try( plot.director.raw(conn.metrics,conn.director) )
  
  dbDisconnect(conn.metrics)
  dbDisconnect(conn.director)
  actions
}

plot.director.raw = function(conn.metrics,conn.director) {
  #png(file=out.file,width=1000,height=600)

	data = get.data(conn.metrics,"ALL","ALL","workload","workload")
	data$time = ISOdate(1970,1,1) + data$time/1000
	t0 = min(data$time,na.rm=T)
	t1 = max(data$time,na.rm=T)
	xtlim = c(t0,t1)
	

	plot.actions(conn.director,xtlim)
	plot.policy.state(conn.director,xtlim)
	plot.all.workload(conn.metrics,xtlim)
	plot.all.latency(conn.metrics,xtlim)
}

plot.all.workload = function(conn.metrics, xtlim) {
	gets = get.data(conn.metrics,"ALL","get","workload","getw")
	puts = get.data(conn.metrics,"ALL","put","workload","putw")
	all = get.data(conn.metrics,"ALL","ALL","workload","allw")
	data = merge( all, merge(gets,puts) )
	data$time = to.date(data$time)
	
	ylim = c(0, max(data[,c("getw","putw","allw")],na.rm=T))
	
	plot( data$time, data$allw, xlim=xtlim, ylim=ylim, bty="n", axes=F, xlab="time", ylab="workload", type="l" )
	axis.POSIXct(1,x=xtlim,format="%H:%M")
	axis(2)
	lines( data$time, data$getw, col="blue" )
	lines( data$time, data$putw, col="red" )
	legend( "bottomleft", legend=c("all","get","put"), col=c("black","blue","red"), inset=0.02, lty=c(1,1,1), lwd=2 )
}

plot.all.latency = function(conn.metrics, xtlim) {
	data = 				get.data(conn.metrics,"ALL","get","latency_mean","get_latency_mean")
	data = merge( data, get.data(conn.metrics,"ALL","get","latency_90p","get_latency_90p") )
#	data = merge( data, get.data(conn.metrics,"ALL","get","latency_99p","get_latency_99p") )
	data = merge( data, get.data(conn.metrics,"ALL","put","latency_mean","put_latency_mean") )
	data = merge( data, get.data(conn.metrics,"ALL","put","latency_90p","put_latency_90p") )
#	data = merge( data, get.data(conn.metrics,"ALL","put","latency_99p","put_latency_99p") )
	data$time = to.date(data$time)

	d = unlist( data[,setdiff(names(data),c("time","timestamp"))] )
	print(d)
	ylim = c(0, quantile(d,0.9,na.rm=T))

	print(data)

	plot( c(), xlim=xtlim, ylim=ylim, bty="n", axes=F, xlab="", ylab="latency [ms]")
	axis.POSIXct(1,x=xtlim,format="%H:%M")
	axis(2)
	lines( data$time, data$get_latency_mean, col="blue", lty=1 )
	lines( data$time, data$get_latency_90p, col="blue", lty=2 )
#	lines( data$time, data$get_latency_99p, col="blue", lty=3 )
	lines( data$time, data$put_latency_mean, col="red", lty=1 )
	lines( data$time, data$put_latency_90p, col="red", lty=2 )
#	lines( data$time, data$put_latency_99p, col="red", lty=3 )
	legend( "bottomleft", legend=c("get mean","get 90p","get 99p","put mean","put 90p","put 99p"), col=c("blue","blue","blue","red","red","red"), inset=0.02, lty=c(1,2,3,1,2,3), lwd=2 )
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

plot.actions = function(conn.director, xtlim) {
	actions = dbGetQuery(conn.director, "select * from actions" )
	actions$update_timeD = ISOdate(1970,1,1) + actions$update_time/1000
	actions$init_timeD = ISOdate(1970,1,1) + actions$init_time/1000
	actions$start_timeD = ISOdate(1970,1,1) + actions$start_time/1000
	actions$end_timeD = ISOdate(1970,1,1) + actions$end_time/1000

	plot( c(), xlim=xtlim, ylim=c(0,1), axes=F, ylab="", xlab="" )
	axis.POSIXct( 1, x=xtlim, format="%H:%M")
	axis(2)	
	for (i in 1:nrow(actions)) {
		y = runif(1,0,1)
		x = c(actions[i,"start_timeD"], actions[i,"end_timeD"])
		lines( x, c(y,y) )
	}
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

get.all.servers = function(c) {
  return( dbGetQuery(c, "select distinct server from scads_metrics")$server )
}

get.data = function(c,server,req.type,stat,col.name) {
  d = dbGetQuery(c, paste("select time as timestamp, value as ",col.name," from scads where metric_id in (select id from scads_metrics where server='",server,"' and request_type='",req.type,"' and stat='",stat,"') ",sep="") )
  d[,col.name] = as.numeric(d[,col.name])
  return(d)
}

