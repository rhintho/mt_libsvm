package de.beuth.inspector

import java.nio.file.{Files, Paths}

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

  def inspectArguments(dataPath: String, maxVeloPath: String, targetPath: String, jamValue: Double,
                       column: Array[String]): Boolean = {
    inspectURL(dataPath) &&
    inspectURL(maxVeloPath) &&
//    inspectURL(targetPath) &&
    inspectJamValue(jamValue) &&
    inspectColumns(column)
  }

  private def inspectJamValue(jamValue: Double): Boolean = {
    jamValue > 0.0 && jamValue <= 1.0
  }

  private def inspectJamValues(jamValue: Array[String]): Boolean = {
    var isValid = true
    for (i <- jamValue.indices) {
      if (!(jamValue.apply(i).toInt > 1)) {
        isValid = false
      }
    }
    isValid
  }

  private def inspectColumns(col: Array[String]): Boolean = {
    var isValid = true
    for (i <- col.indices) {
      if (!verifyColumn(col.apply(i))) {
        isValid = false
      }
    }
    isValid
  }

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

  private def inspectSensorId(sensorId: Int): Boolean = {
    sensorId >= 182 && sensorId <= 1222
  }

  private def inspectTargetPath(targetPath: String): Boolean = {
    !targetPath.contains(".") && Files.exists(Paths.get(targetPath))
  }

  private def inspectTimeInterval(timeInterval: Int): Boolean = {
    timeInterval > 0 && timeInterval < 60
  }

  private def inspectSensorType(sensorType: String): Boolean = {
    sensorType.toUpperCase.equals("PZS") || sensorType.toUpperCase.equals("ABA")
  }

  private def inspectURLs(url: Array[String]): Boolean = {
    var isValid = true
    for (i <- url.indices) {
      if (!Files.exists(Paths.get(url.apply(i)))) {
        isValid = false
      }
    }
    isValid
  }

  private def inspectURL(url: String): Boolean = {
    Files.exists(Paths.get(url))
  }
}
