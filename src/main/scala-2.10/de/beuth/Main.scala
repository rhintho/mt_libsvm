package de.beuth

import de.beuth.inspector.ArgumentInspector

/**
  * Created by Sebastian Urbanek on 05.02.17.
  */
object Main extends App {
  override def main(args: Array[String]): Unit = {
    try {
      // Parameter auslesen
      val dataPath = args.apply(0)
      val maxVeloPath = args.apply(1)
      val targetPath = args.apply(2)
      val jamValue = args.apply(3).toDouble
      val column = readArgument(args.apply(4))

      if (ArgumentInspector.inspectArguments(dataPath, maxVeloPath, targetPath, jamValue, column)) {
        // Argumente sind alle gültig, Transformation kann gestartet werden
        System.out.println("Everything is ok with the arguments!")
        // Starten der Transformation ins Vektorformat
        LIBSVMTransformator.startTransformation(dataPath, maxVeloPath, targetPath, jamValue, column)

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

  private def readArgument(a: String): Array[String] = {
    a.split(";")
  }
}
