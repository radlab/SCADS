package edu.berkeley.cs.scads
package config

import java.io.File

import net.lag.{ configgy, logging }
import configgy.{ Config => CConfig, Configgy }
import logging.Logger

import scala.util.Properties

object Config {

  /**
   * The SCADS Configuration instance
   * Is lazily loaded once
   */
  lazy val config =
    getConfig()

  private def getConfig(): CConfig = {
    // 1) check $SCADS_CONFIG
    // 2) check -Dscads.config
    val filePath0 = 
      Properties
        .envOrNone("SCADS_CONFIG")
        .orElse(
            Properties.propOrNone("scads.config"))

    if (filePath0.isEmpty)
      Console.err.println("""
      | WARNING: Could not find SCADS configuration file to load. 
      | Options are:
      |   (1) Set environmental variable SCADS_CONFIG to the config file path
      |   (2) Set system property scads.config to the config file path
      | Using default values for config.
      """.stripMargin)

    val filePath =
      filePath0.flatMap(fp => {
        val f = new File(fp)
        if (!f.isFile) {
          Console.err.println("""
          | WARNING: File '%s' is not a valid file path
          | Using default values for config
          """.format(fp).stripMargin)
          None
        } else Some(fp)
      })

    filePath.map(fp => { Configgy.configure(fp); Configgy.config }).getOrElse(new CConfig)
  }

  /**
   * Get Logger for name
   */
  def getLog(name: String): Logger = 
    Logger.get(name)

}
