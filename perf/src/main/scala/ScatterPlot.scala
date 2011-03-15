package edu.berkeley.cs
package scads
package perf

import scala.xml._

case class ScatterPlot(points: Seq[(Int, Int)],
		       title: Option[String] = None,
		       subtitle: Option[String] = None,
		       xaxis: Option[String] = None,
		       yaxis: Option[String] = None,
		       xunit: Option[String] = None,
		       yunit: Option[String] = None
		       ) {
  
  protected def quote(str: String) = "'" + str +"'"

  def toHtml: NodeSeq =
    <script type="text/javascript">
      {
"""var chart;
$(document).ready(function() {
   chart = new Highcharts.Chart({
      chart: {
         renderTo: 'chart', 
         defaultSeriesType: 'scatter',
         zoomType: 'xy'
      },
      title: {
         text:  """ + quote(title.getOrElse("")) + """
      },
      subtitle: {
         text: """ + quote(subtitle.getOrElse("")) + """
      },
      xAxis: {
         title: {
            enabled: true,
            text: """ + quote(xaxis.getOrElse("")) + """
         },
         startOnTick: true,
         endOnTick: true,
         showLastLabel: true
      },
      yAxis: {
         title: {
            text: """ + quote(yaxis.getOrElse("")) + """
         }
      },
      tooltip: {
         formatter: function() {
                   return ''+
               this.x + """ + quote(xunit.getOrElse("")) +"""+ ', '+ this.y +"""+ quote(yunit.getOrElse("")) +""";
         }
      },
      legend: {
         layout: 'vertical',
         align: 'left',
         verticalAlign: 'top',
         x: 100,
         y: 70,
         floating: true,
         backgroundColor: '#FFFFFF',
         borderWidth: 1
      },
      plotOptions: {
         scatter: {
            marker: {
               radius: 5,
               states: {
                  hover: {
                     enabled: true,
                     lineColor: 'rgb(100,100,100)'
                  }
               }
            },
            states: {
               hover: {
                  marker: {
                     enabled: false
                  }
               }
            }
         }
      },
      series: [{
         name: 'Female',
         color: 'rgba(223, 83, 83, .5)',
         data: [""" + points.map(p => "[" + p._1 + ", " + p._2 + "]").mkString(",") + """ ]
   
      }]
   });
   
   
});"""
      }
</script>
}
