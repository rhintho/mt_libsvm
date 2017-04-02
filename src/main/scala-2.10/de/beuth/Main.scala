package de.beuth

import de.beuth.inspector.ArgumentInspector

/**
  * Haupteinstiegspunkt des Programms
  */
object Main extends App {
  var label = "X"

  /**
    * Erste Funktion, die nach dem Start des Programms aufgerufen wird.
    * @param args Argumente, die beim Programmaufruf übergeben werden
    */
  override def main(args: Array[String]): Unit = {
    try {
      // Parameter auslesen
      val dataPath = args.apply(0)
      val maxVeloPath = args.apply(1)
      val targetPath = args.apply(2)
      val jamValue = args.apply(3).toDouble
      val column = readArgument(args.apply(4))
      var startTimeInterval = "2009-01-01T00:00:00"
      var endTimeInterval = "2014-12-31T23:59:59"

      // Variabler Zeitraum wird nur eingefügt, wenn angegeben bei der Parameterliste
      if (args.length >= 7) {
        startTimeInterval = args.apply(5)
        endTimeInterval = args.apply(6)
      }
      this.label = args.apply(7)

      // Überprüfung der Argumente
      if (ArgumentInspector.inspectArguments(dataPath, maxVeloPath, targetPath, jamValue, column, startTimeInterval,
                                             endTimeInterval)) {
        // Argumente sind alle gültig, Transformation kann gestartet werden
        System.out.println("Everything is ok with the arguments!")
        // Starten der Transformation ins Vektorformat
        LIBSVMTransformator.startTransformation(dataPath, maxVeloPath, targetPath, jamValue, column, startTimeInterval,
                                                endTimeInterval)

      } else {
        // Argumente ungültig. Fehler ausgeben und weitere Bearbeitung beenden.
        System.err.println("Arguments invalid!")
        System.err.println(ArgumentInspector.errorMessage)
      }
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        System.err.println("No arguments found.\n" + ArgumentInspector.errorMessage)
    }
  }

  /**
    * Trennt eine Zeichenkette mit allen Vektorattributen und überführt sie
    * in ein Array vom Typ String.
    * @param a Zeichenkette mit allen Vektorattributen
    * @return Array mit allen Vektorattributen
    */
  private def readArgument(a: String): Array[String] = {
    a.split(";")
  }
}
