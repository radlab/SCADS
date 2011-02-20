package edu.berkeley.cs.radlab.demo

import java.sql.DriverManager
import java.util.Properties
import java.io.{BufferedReader, InputStreamReader}

object MesosClusterShareReporter {
  def main(args: Array[String]) {
    if (args.length != 1) {
      System.err.println("Usage: UploadLogs <connstring>")
      System.exit(1)
    }
    val connstring = args(0)

    // Load MySQL JDBC driver
    Class.forName("com.mysql.jdbc.Driver")

    // Create a connection

    val conn = DriverManager.getConnection(connstring)

    println("Connected to database!")

    val stmt = conn.prepareStatement("""
      insert into mesosStats
        (timestamp, frameworkId, frameworkName, cpus, gbRam,
         cpuShare, memShare, domShare)
      values (?, ?, ?, ?, ?, ?, ?, ?)
      """)

    val in = new BufferedReader(new InputStreamReader(System.in))
    while (true) {
      val line = in.readLine()
      if (line == null) {
        println("EOF reached, exiting")
        System.exit(0)
      }
      line.trim.split('\t') match {
        case Array(time, fid, name, cpus, mem, cpuShare, memShare, domShare) =>
          stmt.setString(1, time)
          stmt.setString(2, fid)
          stmt.setString(3, name)
          stmt.setInt(4, cpus.toInt)
          stmt.setInt(5, mem.toInt / 1024)
          stmt.setDouble(6, cpuShare.toDouble)
          stmt.setDouble(7, memShare.toDouble)
          stmt.setDouble(8, domShare.toDouble)
          stmt.executeUpdate()
        case _ =>
          System.err.println("Malformed line: " + line)
      }
    }
  }
}
