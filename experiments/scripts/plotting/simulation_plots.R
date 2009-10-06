

SERVER.COST = 1
VIOLATION.COST = 100


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

