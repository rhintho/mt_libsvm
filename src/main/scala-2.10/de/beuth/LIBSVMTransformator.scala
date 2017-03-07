package de.beuth


import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sebastian Urbanek on 05.02.17.
  */
object LIBSVMTransformator {
  // Datenformat
  val csvFormat: String = "com.databricks.spark.csv"

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("LIBSVM Transformator")
  log.setLevel(Level.DEBUG)

  def startTransformation(dataPath: String, maxVeloPath: String, targetPath: String, jamValue: Double,
                          column: Array[String], startTimeInterval: String, endTimeInterval: String): Unit = {
    log.debug("Start der Transformation wird eingeleitet ...")

    // Allgemeine Werte setzen
    // aktuell noch keine Werte zu setzen

    // Parameter preparieren
    val startTimestamp = Timestamp.valueOf(startTimeInterval.replace("T", " "))
    val endTimestamp = Timestamp.valueOf(endTimeInterval.replace("T", " "))

    // Spark initialisieren
    val conf = new SparkConf().setAppName("MT_LIBSVMTransformation")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    // Dataframe erzeugen
    val df = createDataFrame(sQLContext, dataPath)
    val maxValues = createMaxVelocityDataFrame(sQLContext, maxVeloPath)

    // LIBSVM erstellen
    combineVectorAttributes(df, maxValues, column, jamValue, targetPath, startTimestamp, endTimestamp)

    log.debug("Bearbeitung der LIBSVM abgeschlossen.")
  }

  private def combineVectorAttributes(dataFrame: DataFrame, maxValues: DataFrame, col: Array[String], jamValue: Double,
                                      targetPath: String, startTimestamp: Timestamp, endTimestamp: Timestamp): Unit = {

    val joinedData = dataFrame
      .join(maxValues, dataFrame("sensor_id").equalTo(maxValues("sensor_id_mv")), "left_outer")
    joinedData.show(200)

    val testDataFrame = joinedData.select("sensor_id", "max_velocity").distinct
    testDataFrame.show(200)

    var i: Int = 0
    val rdd = joinedData.rdd
      .filter(r => {
        val t = Timestamp.valueOf(r.getString(1))
        (t.equals(startTimestamp) || t.after(startTimestamp)) && (t.equals(endTimestamp) || t.before(endTimestamp))
      })
      .map(r => (
        if (r.getString(3).toDouble < (r.getString(10).toDouble * jamValue) && r.getString(3).toDouble != 0) 0 else 1,
        if (col.contains("sensor_id")) {
          i += 1
          registerVectorAttribute(i, r.getString(0))
        },
        if (col.contains("timestamp")) {
          i += 1
          registerVectorAttribute(i, Timestamp.valueOf(r.getString(1)).getTime.toString)
        },
        if (col.contains("year")) {
          i += 1
          registerVectorAttribute(i, getYear(r.getString(1)).toString)
        },
        if (col.contains("month")) {
          i += 1
          registerVectorAttribute(i, getMonth(r.getString(1)).toString)
        },
        if (col.contains("day")) {
          i += 1
          registerVectorAttribute(i, getDay(r.getString(1)).toString)
        },
        if (col.contains("hour")) {
          i += 1
          registerVectorAttribute(i, getHour(r.getString(1)).toString)
        },
        if (col.contains("minute")) {
          i += 1
          registerVectorAttribute(i, getMinute(r.getString(1)).toString)
        },
        if (col.contains("weekday")) {
          i += 1
          registerVectorAttribute(i, getWeekday(r.getString(1)).toString)
        },
        if (col.contains("period")) {
          i += 1
          registerVectorAttribute(i, getPeriod(r.getString(1)).toString)
        },
        if (col.contains("registration")) {
          i += 1
          registerVectorAttribute(i, r.getString(2))
        },
        if (col.contains("velocity")) {
          i += 1
          registerVectorAttribute(i, r.getString(3))
        },
        if (col.contains("rainfall")) {
          i += 1
          registerVectorAttribute(i, r.getString(4))
        },
        if (col.contains("temperature")) {
          i += 1
          registerVectorAttribute(i, r.getString(5))
        },
        if (col.contains("completeness")) {
          i += 1
          registerVectorAttribute(i, r.getString(8))
        },
        if (col.contains("latitude")) {
          i += 1
          registerVectorAttribute(i, r.getString(6))
        },
        if (col.contains("longitude")) {
          i += 1
          registerVectorAttribute(i, r.getString(7))
        },
        i = 0
      ))
      // Stringmanipulations zur Transformation ins LIBSVM-Format
      .map(r => r.toString().replace("(", "").replace(")", ""). replace(",", " "))

    // Datei rausschreiben mit aktuellem Datum und Uhrzeit als Dateiname
    val dt = LocalDateTime.now
    rdd.saveAsTextFile(targetPath + (dt.getYear - 2000) + "-" + dt.getMonthValue + "-" + dt.getDayOfMonth + "T" +
                          dt.getHour + dt.getMinute + ".libsvm")
    log.debug("Datei erfolgreich herausgeschrieben. ")
  }

  private def createDataFrame(sQLContext: SQLContext, url: String): DataFrame = {
    // CSV-Datei einlesen und in DataFrame überführen
    sQLContext
      .read
      .format(this.csvFormat)
      .option("header", "false")
      .option("inferSchema", "false")
      .load(url)
    // Einzelne Spalten ordentlich umbenennen
      .withColumnRenamed("C0", "sensor_id")
      .withColumnRenamed("C1", "timestamp")
      .withColumnRenamed("C2", "registration")
      .withColumnRenamed("C3", "velocity")
      .withColumnRenamed("C4", "rainfall")
      .withColumnRenamed("C5", "temperature")
      .withColumnRenamed("C6", "latitude")
      .withColumnRenamed("C7", "longitude")
      .withColumnRenamed("C8", "completeness")
  }

  private def createMaxVelocityDataFrame(sQLContext: SQLContext, url: String) = {
    sQLContext
      .read
      .format(this.csvFormat)
      .option("header", "false")
      .option("inferSchema", "false")
      .load(url)
      .withColumnRenamed("C0", "sensor_id_mv")
      .withColumnRenamed("C1", "max_velocity")
  }

  private def registerVectorAttribute(i: Int, v: String): String = {
    i + ":" + v
  }

  private def getPeriod(timestamp: String): Int = {
    val hour = getHour(timestamp)
    if (6 <= hour && hour < 12) {
      // Vormittag 6 - 12 Uhr
      1
    } else if (12 <= hour && hour < 18) {
      // Nachmittag 12 - 18 Uhr
      2
    } else if (18 <= hour && hour <= 23) {
      // Abend / Nacht 18 - 0 Uhr
      3
    } else {
      // Früher Morgen 0 - 6 Uhr
      4
    }
  }

  private def getWeekday(timestamp: String): Int = {
    val isoTimestamp = timestamp.replace(" ", "T")
    LocalDateTime.parse(isoTimestamp).getDayOfWeek.getValue
  }

  private def getMinute(timestamp: String): Int = {
    timestamp.substring(14, 16).toInt
  }

  private def getHour(timestamp: String): Int = {
    timestamp.substring(11, 13).toInt
  }

  private def getDay(timestamp: String): Int = {
    timestamp.substring(8, 10).toInt
  }

  private def getMonth(timesramp: String): Int = {
    timesramp.substring(5, 7).toInt
  }

  private def getYear(timestamp: String): Int = {
    timestamp.substring(0, 4).toInt
  }
}
