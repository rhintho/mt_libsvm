package de.beuth.inspector

import java.nio.file.{Files, Paths}
import java.sql.Timestamp

/**
  * Objekt zur Überprüfung der einzelnen Argumente bei Programmaufruf. Sollten diese Parameter bereits nicht
  * stimmen, ist es sinnvoll die weitere Bearbeitung abzubrechen und eine Nachricht an den Nutzer zu senden.
  */
object ArgumentInspector {

  val errorMessage: String = "Please use following options after spark-submit JAR " +
                             "[path to data] " +
                             "[path to target] " +
                             "[jam value] " +
                             "[columns] " +
                             "\nMore arguments split with semikolons (;)" +
                             "\nAllowed columns are sensor_id, timestamp, registration, velocity, rainfall, " +
                             "temperature, latitude, longitude and completeness "

  /**
    * Öffentliche Funktion, die alle übergebenen Parameter hinsichtlich ihrer Korrektheit überprüft.
    * @param dataPath Pfad zu der aggregierten CSV-Datei
    * @param maxVeloPath Pfad zur CSV-Datei mit den Höchstgeschwindigkeitsgrenzen für die mit Sensoren belegten
    *                    Straßen
    * @param targetPath Pfad im Dateisystem, wo die LIBSVM erzeugt werden soll
    * @param jamValue Stauuungsfaktor, der die Klassifikation anhand der Höchstgeschwindigkeit errechnet
    * @param column Ausgewählte Vektorattribute
    * @param startTimeInterval Startzeitpunkt für Daten der Ziel-LIBSVM
    * @param endTimeInterval Endzeitpunkt für Daten in der LIBSVM
    * @return
    */
  def inspectArguments(dataPath: String, maxVeloPath: String, targetPath: String, jamValue: Double,
                       column: Array[String], startTimeInterval: String, endTimeInterval: String): Boolean = {
    inspectURL(dataPath) &&
    inspectURL(maxVeloPath) &&
    inspectJamValue(jamValue) &&
    inspectColumns(column) &&
    inspectTimeInterval(startTimeInterval) &&
    inspectTimeInterval(endTimeInterval)
  }

  /**
    * Überprüft einen Zeitstempel, ob der in einem gültigen Zeitbereich liegt.
    * @param value Zeitstempel
    * @return Boolean ob Zeitstempel in erlaubtem Bereich liegt
    */
  private def inspectTimeInterval(value: String): Boolean = {
    val minTimestamp = Timestamp.valueOf("2009-01-01 00:00:00")
    val maxTimestamp = Timestamp.valueOf("2015-12-31 23:59:59")
    val formattedValue = value.replace("T", " ")
    val timestamp = Timestamp.valueOf(formattedValue)
    (timestamp.equals(minTimestamp) || timestamp.after(minTimestamp)) &&
    (timestamp.equals(maxTimestamp) || timestamp.before(maxTimestamp))
  }

  /**
    * Überprüfung des Stauungsfaktors auf Korrektheit.
    * @param jamValue Stauungsfaktor
    * @return true, wenn Stauungsfaktor zwischen 0 und 1, sonst false
    */
  private def inspectJamValue(jamValue: Double): Boolean = {
    jamValue > 0.0 && jamValue <= 1.0
  }

  /**
    * Überprüfung auf richtige Schreibweise aller Vektorattribute
    * @param col Array mit allen Vektorattributen
    * @return true, wenn alle Vektorattribute korrekt geschrieben wurden, sonst false
    */
  private def inspectColumns(col: Array[String]): Boolean = {
    var isValid = true
    for (i <- col.indices) {
      if (!verifyColumn(col.apply(i))) {
        isValid = false
      }
    }
    isValid
  }

  /**
    * Überprüfung, ob übergebenes Vektorattribut erlaubt ist.
    * @param col Vektorattribut
    * @return true, wenn Vektorattribut erlaubt, sonst false.
    */
  private def verifyColumn(col: String): Boolean = {
    col match {
      case "sensor_id" => true
      case "timestamp" => true
      case "registration" => true
      case "velocity" => true
      case "rainfall" => true
      case "temperature" => true
      case "latitude" => true
      case "longitude" => true
      case "completeness" => true
      case "year" => true
      case "month" => true
      case "day" => true
      case "hour" => true
      case "minute" => true
      case "weekday" => true
      case "period" => true
      case _ => false
    }
  }

  /**
    * Überprüfung, ob Zielpfad existiert und lesbar ist.
    * @param url Dateipfad zur Überprüfung
    * @return true, wenn Dateipfad existent und lesber, sonst false.
    */
  private def inspectURL(url: String): Boolean = {
    Files.exists(Paths.get(url)) && Files.isReadable(Paths.get(url))
  }
}
