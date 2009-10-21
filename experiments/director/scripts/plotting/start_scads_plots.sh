#!/bin/bash

echo "source('/opt/scads/experiments/scripts/plotting/scads_plots.R'); while(T) { try(plot.scads.performance('/var/www/performance.png')); Sys.sleep(20) } " | R --vanilla &> /var/log/scads_plots.log