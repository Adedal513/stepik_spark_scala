package com.example.jobs

import com.example.config.AppConfig
import com.example.processing.MetaWriter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.log4j.LogManager


object SampleJob {

    def readCsvFile(spark: SparkSession, filePath: String, cached: Boolean = true): DataFrame = {
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(filePath)
    }

    def run(spark: SparkSession, config: AppConfig, order: String = "asc", use_cache: Boolean) {

        val log = LogManager.getRootLogger

        val sortFunctions = Map(
            "asc" -> asc _,
            "desc" -> desc _
        )

        val sortFunction = sortFunctions.getOrElse(
            order.toLowerCase,
            throw new IllegalArgumentException(s"Invalid sort order: $order. Use 'asc' or 'desc'.")
        )

        if (use_cache) {
            log.info(s"Cache is on. Reading cache from path ${config.cachePath}/")
        }

        val airlinesPath = config.datasetPaths("airlines")
        val airportsPath = config.datasetPaths("airports")
        val flightsPath = config.datasetPaths("flights")

        val airlinesDF = readCsvFile(spark, airlinesPath)
        val airportsDF = readCsvFile(spark, airportsPath)
        val flightsDF = readCsvFile(spark, flightsPath)

        log.info("Successfully parsed all needed datasets.")

        // 1. Топ-10 самых популярных аэропортов по количеству совершаемых полетов

        val topAirports = flightsDF
            .groupBy("ORIGIN_AIRPORT")
            .agg(count("FLIGHT_NUMBER").alias("flight_count"))
            .orderBy(sortFunction("flight_count"))
        
        println("Топ-10 самых популярных аэропортов:")
        topAirports.limit(10).show()

        // 2. Топ-10 авиакомпаний, вовремя выполняющих рейсы
        val topOnTimeAirlines = flightsDF
            .filter(col("ARRIVAL_DELAY") <= 0)
            .groupBy("AIRLINE")
            .agg(count("FLIGHT_NUMBER").alias("on_time_flights"))
            .join(airlinesDF, flightsDF("AIRLINE") === airlinesDF("IATA_CODE"))
            .select(flightsDF("AIRLINE").alias("AIRLINE_CODE"), airlinesDF("AIRLINE").alias("AIRLINE_NAME"), col("on_time_flights"))
            .orderBy(sortFunction("on_time_flights"))

        println("Топ-10 авиакомпаний, вовремя выполняющих рейсы:")
        topOnTimeAirlines.limit(10).show()

        // 3. Топ-10 перевозчиков и аэропортов назначения для каждого аэропорта
        val RawTopCarriersDestinations = flightsDF
            .filter(col("DEPARTURE_DELAY") <= 0)
            .groupBy("ORIGIN_AIRPORT", "AIRLINE", "DESTINATION_AIRPORT")
            .agg(count("FLIGHT_NUMBER").alias("on_time_departures"))
            .withColumn("rank", rank().over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(desc("on_time_departures"))))
            .orderBy(sortFunction("rank"))

        val topCarriersDestinations = RawTopCarriersDestinations
            .withColumn("rank", rank().over(Window.partitionBy("ORIGIN_AIRPORT").orderBy(desc("on_time_departures"))))
            .orderBy(sortFunction("rank"))
            .filter(col("rank") <= 10)
        
        println("Топ-10 перевозчиков и аэропортов назначения для каждого аэропорта:")
        topCarriersDestinations.show()

        // 4. Дни недели в порядке своевременности прибытия рейсов
        val onTimeByDayOfWeek = flightsDF
            .withColumn("on_time", when(col("ARRIVAL_DELAY") <= 0, 1).otherwise(0))
            .groupBy("DAY_OF_WEEK")
            .agg(avg("on_time").alias("on_time_rate"))
            .orderBy(sortFunction("on_time_rate"))

        println("Дни недели в порядке своевременности прибытия рейсов:")
        onTimeByDayOfWeek.show()

        // 5. Количество задержанных рейсов по причинам
        val delayReasons = flightsDF
            .select(
                sum(when(col("AIR_SYSTEM_DELAY").isNotNull, 1).otherwise(0)).alias("AIR_SYSTEM_DELAY"),
                sum(when(col("SECURITY_DELAY").isNotNull, 1).otherwise(0)).alias("SECURITY_DELAY"),
                sum(when(col("AIRLINE_DELAY").isNotNull, 1).otherwise(0)).alias("AIRLINE_DELAY"),
                sum(when(col("LATE_AIRCRAFT_DELAY").isNotNull, 1).otherwise(0)).alias("LATE_AIRCRAFT_DELAY"),
                sum(when(col("WEATHER_DELAY").isNotNull, 1).otherwise(0)).alias("WEATHER_DELAY")
            )

        println("Количество рейсов с задержками по причинам:")
        delayReasons.show()

        // 6. Процентное соотношение минут задержки по причинам
        val delayPercentages = flightsDF
            .select(
                sum(col("AIR_SYSTEM_DELAY")).alias("AIR_SYSTEM_DELAY"),
                sum(col("SECURITY_DELAY")).alias("SECURITY_DELAY"),
                sum(col("AIRLINE_DELAY")).alias("AIRLINE_DELAY"),
                sum(col("LATE_AIRCRAFT_DELAY")).alias("LATE_AIRCRAFT_DELAY"),
                sum(col("WEATHER_DELAY")).alias("WEATHER_DELAY")
            )
            .withColumn("total_delay", col("AIR_SYSTEM_DELAY") + col("SECURITY_DELAY") + col("AIRLINE_DELAY") + col("LATE_AIRCRAFT_DELAY") + col("WEATHER_DELAY"))
            .select(
                (col("AIR_SYSTEM_DELAY") / col("total_delay") * 100).alias("AIR_SYSTEM_DELAY_%"),
                (col("SECURITY_DELAY") / col("total_delay") * 100).alias("SECURITY_DELAY_%"),
                (col("AIRLINE_DELAY") / col("total_delay") * 100).alias("AIRLINE_DELAY_%"),
                (col("LATE_AIRCRAFT_DELAY") / col("total_delay") * 100).alias("LATE_AIRCRAFT_DELAY_%"),
                (col("WEATHER_DELAY") / col("total_delay") * 100).alias("WEATHER_DELAY_%")
            )

        println("Процентное соотношение минут задержки по причинам:")
        delayPercentages.show()

        val metaWriter = new MetaWriter(config.metaPath)

        metaWriter.gatherAndSave(
            spark= spark,
            df = flightsDF,
            dateColumn= "YEAR",
            name= "flights_analysis"
        )

        spark.stop()
    }
}

