

plot.wikipedia.buckets = function(data) {
	n = ncol(data)-1
	
	colors = rainbow(n)
	
	plot(data[,2],type="l",bty="n",ylim=c(0,max(data[,2:ncol(data)])),col=colors[1],
		xlab="hours", ylab="hits/hour")
	for (i in 3:(n+1)) lines(data[,i],col=colors[i-1])
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