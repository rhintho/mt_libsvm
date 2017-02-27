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

  def startTransformation(dataPath: String, targetPath: String, jamValue: Int, column: Array[String]): Unit = {
    log.debug("Start der Transformation wird eingeleitet ...")

    // Allgemeine Werte setzen
    // aktuell noch keine Werte zu setzen

    // Spark initialisieren
    val conf = new SparkConf().setAppName("MT_LIBSVMTransformation")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    // Dataframe erzeugen
    val df = createDataFrame(sQLContext, dataPath)

    // LIBSVM erstellen
    combineVectors(df, column, jamValue, targetPath)

    log.debug("Bearbeitung der LIBSVM abgeschlossen.")
  }

  private def combineVectors(dataFrame: DataFrame, col: Array[String], jamValue: Int, targetPath: String): Unit = {
//    var i: Int = 1
    val rdd = dataFrame.rdd
      .map(r => (
        if (r.getString(3).toDouble < jamValue && r.getString(2).toDouble != 0) 0 else 1,
        if (col.contains("sensor_id")) 1 + ":" + r.getString(0) else 1 + ":" + 0,
        if (col.contains("timestamp")) 2 + ":" + Timestamp.valueOf(r.getString(1)).getTime  else 2 + ":" + 0,
        if (col.contains("year")) 3 + ":" + getYear(r.getString(1)) else 3 + ":" + 0,
        if (col.contains("month")) 4 + ":" + getMonth(r.getString(1)) else 4 + ":" + 0,
        if (col.contains("day")) 5 + ":" + getDay(r.getString(1)) else 5 + ":" + 0,
        if (col.contains("hour")) 6 + ":" + getHour(r.getString(1)) else 6 + ":" + 0,
        if (col.contains("minute")) 7 + ":" + getMinute(r.getString(1)) else 7 + ":" + 0,
        if (col.contains("weekday")) 8 + ":" + getWeekday(r.getString(1)) else 8 + ":" + 0,
        if (col.contains("period")) 9 + ":" + getPeriod(r.getString(1)) else 9 + ":" + 0,
        if (col.contains("registration")) 10 + ":" + r.getString(2).toDouble.toInt else 10 + ":" + 0,
        if (col.contains("velocity")) 11 + ":" + r.getString(3) else 11 + ":" + 0,
        if (col.contains("rainfall")) 12 + ":" + r.getString(4) else 12 + ":" + 0,
        if (col.contains("temperature")) 13 + ":" + r.getString(5) else 13 + ":" + 0,
        if (col.contains("completeness")) 14 + ":" + r.getString(8) else 14 + ":" + 0,
        if (col.contains("latitude")) 15 + ":" + r.getString(6) else 15 + ":" + 0,
        if (col.contains("longitude")) 16 + ":" + r.getString(7) else 16 + ":" + 0
      ))
      // String-Formatierung zur Überführung in LIBSVM-Format
      .map(r => r.toString().replace("(", "").replace(")", ""). replace(",", " "))
    val dt = LocalDateTime.now
    rdd.saveAsTextFile(targetPath + (dt.getYear - 2000) + "-" + dt.getMonthValue + "-" + dt.getDayOfMonth + "T" +
                          dt.getHour + "." + dt.getMinute + "." + dt.getSecond + ".libsvm")
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
