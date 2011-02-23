#!/bin/bash
hostname > host.out
while true;  do
	echo "plotting at" `date`
	R -f makeplots.r
	R -f mesosplots.r
	mv *.png ..
	sleep 30
done
