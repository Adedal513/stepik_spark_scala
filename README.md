##   Скрипт анализа полётов

### Конфигурация проекта

Для работы скрипту необходим файл конфигурации.

```conf
# Название приложения и адрес мастера
app {
  name = "MySparkApp",
  master = "local[*]"
}

# Путь до наборов данных
datasets {
  airports = "resources/data/project/airports.csv",
  flights = "resources/data/project/flights.csv",
  airlines = "resources/data/project/airlines.csv"
}

# Папка для сохранения кешированных вычислений
cache {
  path = ".cache"
}

# Путь сохранения кешированной
meta {
  path = ".meta"
}
```

Путь к которому казывается через параметр `--config` при запуске:

```
spark-submit final.jar --config config.conf <...>
```
### Использование:

Для запуска скрипта необходимо собрать проект через `sbt` и запустить его на spark кластере.

Перейдите в директорию с репозиторием и соберите проект
```
cd stepik_spark_scala/
sbt assembly
```
В результате будет получен файл `final.jar` в пути `target/scala-<version>/`.
Полученный код можно запустить, используя команду `spark-submit`:
```
spark-submit --class com.example.FlightAnalyzer target/scala-2.12/final.jar --config-path "flight_analyzer.conf" --order "desc" --cache
```
### Помощь

В случае необходимости, помощь можно получить при помощи команды `help`:

```
spark-submit --class com.example.FlightAnalyzer target/scala-2.12/final.jar help 
```