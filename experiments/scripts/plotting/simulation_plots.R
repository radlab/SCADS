

SERVER.COST = 1
VIOLATION.COST = 100

history = function() {
	d = read.csv("~/Downloads/scads/simulation/simulation_search_down0.csv")
	plot.tradeoff(d,"p.alpha_down",logx=T)
	d = read.csv("~/Downloads/scads/simulation/simulation_search_safety0.csv")
	plot.tradeoff(d,"p.safety",logx=F)
	d = read.csv("~/Downloads/scads/simulation/simulation_search_up0.csv")
	plot.tradeoff(d,"p.alpha_up",logx=F)
	head(d)
	dim(d)
	d
}

superpose.eb <- function (x, y, ebl, ebu = ebl, length = 0.08, ...) {
	arrows(x, y + ebu, x, y - ebl, angle = 90, code = 3, length = length, ...)
}

plot.tradeoff = function(data, x, y1="nServerUnits", y2="percentageSlow", logx=FALSE) {
	params = grep( "p\\.", colnames(data) )
	x.param = grep( x, colnames(data))

	d.mean = aggregate( d, by=list(x=d[,x]), FUN=mean)
	d.mean = d.mean[ order(d.mean$x), ]
	d.sd = aggregate( d, by=list(x=d[,x]), FUN=sd)
	d.sd = d.sd[ order(d.sd$x), ]
	
	xd = sort( unique( d[,x] ) )
	
	logxs = ""
	if (logx) logxs = "x"
	
	y1lim = c(0,max(data[,y1]))
	if (y1=="percentageSlow") y1lim = c(0,1)
	y2lim = c(0,max(data[,y2]))
	if (y2=="percentageSlow") y2lim = c(0,1)
	
	print( d.mean[,c(x,y1,y2)])
	
	par(mar=c(4, 4, 2, 4) + 0.1)
	par(mgp=c(2,0.8,0))
	plot( xd, d.mean[,y1], type="b", col="blue", ylim=y1lim, xlab=x, ylab="", bty="n", yaxt="n", main=paste(y1," and ",y2,sep=""), log=logxs )
	superpose.eb( xd, d.mean[,y1], d.sd[,y1], length=0.05, col="blue" )
	axis(2, col.axis="blue", las=2)
	mtext(y1, side=2, line=2.3, cex.lab=1, las=3, col="blue")

	par(new=T)
	plot( xd, d.mean[,y2], axes=F, type="b", col="red", ylim=y2lim, xlab="", ylab="", bty="n", log=logxs)
	axis(4, col.axis="red", las=2)
	superpose.eb( xd, d.mean[,y2], d.sd[,y2], length=0.05, col="red" )
	mtext(y2, side=4, line=2, cex.lab=1, las=3, col="red")

}


plot.slice = function(data, x) {
	params = grep( "p\\.", colnames(data) )
	x.param = grep( x, colnames(data))
	
	split.screen( screen.coord(c(3,2)) )

	screen(1)
	par(mar=c(4, 4, 2, 4) + 0.1)
	par(mgp=c(2,0.8,0))
	plot( data[,x], data[,"nServerUnits"], type="b", col="blue", ylim=c(0,max(data[,"nServerUnits"])), xlab=x, ylab="", bty="n", yaxt="n", main="number of servers and SLA violations" )
	axis(2, col.axis="blue", las=2)
	mtext("# server units", side=2, line=2.3, cex.lab=1, las=3, col="blue")

	par(new=T)
	plot( data[,x], data[,"nSLAViolations"], axes=F, type="b", col="red", ylim=c(0,max(data[,"nSLAViolations"])), xlab="", ylab="", bty="n")
	axis(4, col.axis="red", las=2)
	mtext("# SLA violations", side=4, line=2, cex.lab=1, las=3, col="red")

	screen(2)
	par(mar=c(4, 4, 2, 4) + 0.1)
	par(mgp=c(2,0.8,0))
	cost = data[,"nServerUnits"]*SERVER.COST+data[,"nSLAViolations"]*VIOLATION.COST
	plot( data[,x], cost, type="b", ylab="cost", xlab=x, bty="n", main=paste("total cost (server=$",SERVER.COST,", violation=$",VIOLATION.COST,")",sep="") )
	points( data[grep( min(cost), cost), x], min(cost), col="green", pch=20 )
	cat( paste("min at ",x,"=",data[grep( min(cost), cost), x],"\n",sep=""))

	close.screen(all=T)
}


screen.coord = function(ysizes) {
	n = length(ysizes)
	s = sum(ysizes)
	matrix(c( rep(0,n), rep(1,n), 1-(cumsum(ysizes)/s), 1-(cumsum(c(0,ysizes[-n]))/s) ), nrow=n )
}

