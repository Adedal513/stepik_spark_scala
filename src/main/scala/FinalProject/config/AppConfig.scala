package com.example.config
import com.typesafe.config.{Config, ConfigFactory}

case class AppConfig(
    appName: String,
    master: String,
    datasetPaths: Map[String, String],
    cachePath: String,
    metaPath: String
)

object AppConfig {
    def load(configPath: String): AppConfig = {
        val config: Config = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

        AppConfig(
            appName = config.getString("app.name"),
            master = config.getString("app.master"),
            cachePath = config.getString("cache.path"),
            metaPath = config.getString("meta.path"),
            datasetPaths = config.getConfig("datasets").entrySet().toArray().map { entry => 
                val key = entry.asInstanceOf[java.util.Map.Entry[String, _]].getKey
                key -> config.getString(s"datasets.$key")
            }.toMap
        )
    }
}

