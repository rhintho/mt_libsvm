package de.beuth


import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Funktionssammlung zur Erstellung einer LIBSVM aus den CSV-Dateien
  * des Programms mt_preanalysis.
  */
object LIBSVMTransformator {
  // Datenformat
  val csvFormat: String = "com.databricks.spark.csv"

  // LogManager initialisieren
  val log: Logger = LogManager.getLogger("LIBSVM Transformator")
  log.setLevel(Level.DEBUG)

  /**
    * Hauptfunktion, welche aufgrund aller Parameter die Erzeugung der LIBSVM
    * übernimmt und sie später in das System als Datei schreibt.
    * @param dataPath Pfad zu der aggregierten CSV-Datei
    * @param maxVeloPath Pfad zur CSV-Datei mit den Höchstgeschwindigkeitsgrenzen für die mit Sensoren belegten
    *                    Straßen
    * @param targetPath Pfad im Dateisystem, wo die LIBSVM erzeugt werden soll
    * @param jamValue Stauuungsfaktor, der die Klassifikation anhand der Höchstgeschwindigkeit errechnet
    * @param column Ausgewählte Vektorattribute
    * @param startTimeInterval Startzeitpunkt für Daten der Ziel-LIBSVM
    * @param endTimeInterval Endzeitpunkt für Daten in der LIBSVM
    */
  def startTransformation(dataPath: String, maxVeloPath: String, targetPath: String, jamValue: Double,
                          column: Array[String], startTimeInterval: String, endTimeInterval: String): Unit = {
    log.debug("Start der Transformation wird eingeleitet ...")

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

  /**
    * Setzt die aggregierten CSV-Daten mit den Höchstgeschwindigkeitsdaten zusammen und erzeugt eine RDD
    * im Format LIBSVM
    * @param dataFrame DataFrame mit Ausgangsdaten (CSV)
    * @param maxValues DataFrame mit
    * @param col Array mit allen Vektorattributen, die in die LIBSVM sollen
    * @param jamValue Stauungsfaktor
    * @param targetPath Zielpfad
    * @param startTimestamp Zeitstempel für LIBSVM
    * @param endTimestamp Zeitstempel für LIBSVM
    */
  private def combineVectorAttributes(dataFrame: DataFrame, maxValues: DataFrame, col: Array[String], jamValue: Double,
                                      targetPath: String, startTimestamp: Timestamp, endTimestamp: Timestamp): Unit = {

    // JOIN der Ausgangsdaten und der Höchstgeschwindigkeitsdaten
    val joinedData = dataFrame
      .join(maxValues, dataFrame("sensor_id").equalTo(maxValues("sensor_id_mv")), "left_outer")

    var i: Int = 0
    val rdd = joinedData.rdd
      // Filter für gegebenen Zeitraum
      .filter(r => {
        val t = Timestamp.valueOf(r.getString(1))
        (t.equals(startTimestamp) || t.after(startTimestamp)) && (t.equals(endTimestamp) || t.before(endTimestamp))
      })
      // Zusammensetz der LIBSVM Informationen
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
      .map(r => r.toString().replace("(", "").replace(")", "").replace(",", " ")
      // Zum Entfernen überflüssiger Leerzeichen
           .replace("  ", " ").replace("  ", " ").replace("  ", " ").replace("  ", " ").replace("  ", " "))

    // Datei rausschreiben mit aktuellem Datum und Uhrzeit als Dateiname
    rdd.saveAsTextFile(targetPath + jamValue + "_" + Main.label)
    log.debug("Datei erfolgreich herausgeschrieben. ")
  }

  /**
    * Erzeugt einen DataFrame aus der vorhandenen, aggegrierten CSV-Datei.
    * @param sQLContext SQLContext von Apache Spark
    * @param url Dateipfad zur CSV
    * @return DataFrame mit Informationen aus CSV
    */
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

  /**
    * Erzeugt ein DataFrame aus den Höchstgeschwindigkeitsdaten
    * @param sQLContext SQLContext von Apache Spark
    * @param url Dateipfad zu den Höchstgeschwindigkeitsdaten
    * @return DataFrame mit den Höchstgeschwindigkeitsdaten
    */
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

  /**
    * Bildet einen String aus einem Index und einem Wert.
    * @param i Index
    * @param v Wert
    * @return Zeichenkette für LIBSVM aus Index:Wert
    */
  private def registerVectorAttribute(i: Int, v: String): String = {
    if (!v.equals("null")) {
      i + ":" + v
    } else {
      i + ":" + 0.0
    }
  }

  /**
    * Liest aus einem Zeitstempel die Tageszeit heraus
    * @param timestamp Zeitstempel
    * @return Tageszeit als Integer-Wert
    */
  private def getPeriod(timestamp: String): Int = {
    val hour = getHour(timestamp)
    if (6 <= hour && hour < 10) {
      // Morgen 6 - 10 Uhr
      1
    } else if (10 <= hour && hour < 14) {
      // Mittag 10 - 14 Uhr
      2
    } else if (14 <= hour && hour < 18) {
      // Nachmittag 14 - 18 Uhr
      3
    } else if (18 <= hour && hour < 22) {
      // Abend 18 - 22 Uhr
      4
    } else if (22 <= hour && hour < 2) {
      // Restliche Nacht 22 - 2 Uhr
      5
    } else {
      // Früher Morgen 2 - 6 Uhr
      6
    }
  }

  /**
    * Liest aus einem Zeitstempel den Wochentag.
    * @param timestamp Zeitstempel
    * @return Wochentag nach Java DateTime API
    */
  private def getWeekday(timestamp: String): Int = {
    val isoTimestamp = timestamp.replace(" ", "T")
    LocalDateTime.parse(isoTimestamp).getDayOfWeek.getValue
  }

  /**
    * Liest aus einem Zeitstempel den Minutenwert
    * @param timestamp Zeitstempel
    * @return Minutenwert
    */
  private def getMinute(timestamp: String): Int = {
    timestamp.substring(14, 16).toInt
  }

  /**
    * Liest den Stundenwert aus einem Zeitstempel
    * @param timestamp Zeitstempel
    * @return Stundenwert
    */
  private def getHour(timestamp: String): Int = {
    timestamp.substring(11, 13).toInt
  }

  /**
    * Liest den Kalendertag aus einem Zeitstempel
    * @param timestamp Zeitstempel
    * @return Tag
    */
  private def getDay(timestamp: String): Int = {
    timestamp.substring(8, 10).toInt
  }

  /**
    * Liest aus einem Zeitstempel den Monat
    * @param timesramp Zeitstempel
    * @return Monat
    */
  private def getMonth(timesramp: String): Int = {
    timesramp.substring(5, 7).toInt
  }

  /**
    * LIest aus einem Zeitstempel das Jahr
    * @param timestamp Zeitstempel
    * @return Jahr
    */
  private def getYear(timestamp: String): Int = {
    timestamp.substring(0, 4).toInt
  }
}
