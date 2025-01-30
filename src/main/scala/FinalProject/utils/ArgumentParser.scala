package com.example.utils

import scopt.OParser


object ArgumentParser {
  case class Arguments(
    configPath: String = "flight_analyzer.conf",
    order: String = "asc",
    use_cache: Boolean = false
  )

  def parseArgs(args: Array[String]): Option[Arguments] = {
    val builder = OParser.builder[Arguments]
    val parser = {
        import builder._

        OParser.sequence(
            programName("Flight Analyzer"),
            head("flight analyzer v0.0.1"),
            opt[String]("config-path")
                .required()
                .action((x, c) => c.copy(configPath = x))
                .text("app config path."),
            opt[String]("order")
                .optional()
                .action((x, c) => c.copy(order = x))
                .text("sort mode for metrics. asc as default."),
            opt[Unit]("cache")
                .optional()
                .action((_, c) => c.copy(use_cache = true))
                .text("to use cache data"),
            note("Requires exactly 3 csv files specified in .conf")
        )
    }
    OParser.parse(parser, args, Arguments())
  }
}