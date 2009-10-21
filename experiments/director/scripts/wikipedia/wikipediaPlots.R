

history = function() {
	wiki10 = read.csv("~/workspace/data/wikipedia/wiki_10buckets.csv")
	plot.wikipedia.buckets(wiki10)

	wiki1 = data.frame( hour=wiki10$hour, all=apply( wiki10[,2:10], 1, sum ) )
	plot.wikipedia.buckets(wiki1)
}

plot.wikipedia.buckets = function(data) {
	n = ncol(data)-1
	
	colors = rainbow(n)
	
	par(cex=1.4)
	plot(data[,2]/1000,type="l",bty="n",ylim=c(0,max(data[,2:ncol(data)]/1000)),col=colors[n],
		xlab="hours", ylab="hits/hour [x 1000]", lwd=3, cex=2)
	for (i in 3:(n+1)) lines(data[,i]/1000,col=colors[n-i+1], lwd=3)
}

seq.of.histograms = function(samples, nbuckets) {

	minx = min(samples)
	maxx = max(samples)

	breaks = seq( minx-1, maxx+1, length=nbuckets+1)
	density = data.frame()

	for (i in 1:nrow(samples)) {
		c = hist( as.numeric(samples[i,]), breaks=breaks, plot=F )
		density = rbind(density,c$density)
		#pause()
	}	

	plot.wikipedia.buckets(density)
	density
}