package com.example

import org.apache.log4j.{Logger, Level}
import com.example.config.AppConfig
import com.example.utils.ArgumentParser
import com.example.utils.SparkSessionBuilder
import com.example.jobs.SampleJob
import org.apache.log4j.LogManager


object FlightAnalyzer {
    def main(args: Array[String]): Unit = {
        val log = LogManager.getRootLogger

        val parsedArgs: Option[ArgumentParser.Arguments] = ArgumentParser.parseArgs(args)
        
        val arguments = parsedArgs.getOrElse {
            println("Invalid arguments provided. Please check the inputs.")
            System.exit(1)
            ArgumentParser.Arguments()
        }

        // Загрузка конфигурации
        val config = AppConfig.load(arguments.configPath)

        log.info(s"Config successfully parsed. Running app with parameters: $config")
        
        // Инициализация Spark-сессии
        val spark = SparkSessionBuilder.buildSession(config.appName, config.master)

        SampleJob.run(spark, config, arguments.order, arguments.use_cache)

        spark.stop()
    }
}