#!/bin/bash

echo "source('/opt/scads/experiments/scripts/plotting/scads_plots.R'); while(T) { try(plot.director.simple(out.file='/var/www/director_simple.png',ts0=-60*60*1000)); Sys.sleep(20); disconnect.all(); } " | R --vanilla &> /var/log/scadsplots_simple_director.log