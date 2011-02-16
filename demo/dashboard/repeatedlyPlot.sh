#!/bin/bash
hostname > host.out
while true;  do
	echo "plotting at" `date`
	R -f makeplots.r
	R -f mesosplots.r
	sleep 60
done
