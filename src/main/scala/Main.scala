object Main {

  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import scala.collection.Map
  import java.io.File

  def getListOfFiles(dir: String): List[File] = {
    (new File(dir)) match {
      case f if f.exists && f.isDirectory => f.listFiles.toList.map(_.toString).flatMap(getListOfFiles(_))
      case f if f.exists && f.isFile => List(f)
      case _ => List[File]()
    }
  }

  // TODO: should i limit these to just root json objects, aka jsonagg and array agg?
  def analyzeJson(json: JValue): ValueAggregate =
    ValueAggregate.fromJValue(json)

  def filesToStrings(files: List[File]): Stream[String] = {
    files match {
      case f :: fs => fileToString(f) #:: filesToStrings(fs)
      case Nil => Stream.empty
    }
  }

  def fileToString(file: File): String = {
    val source = io.Source.fromFile(file)
    try source.mkString finally source.close
  }

  def main(args: Array[String]) = {
    println("input:")
    println(args.mkString("; ") + "\n") // /Users/victor/code/little-brother/114th_bulk_data/bills, out.json

    val dir = args(0)
    val out = args(1)
    val limit = args(2).toInt

    val files: List[File] = getListOfFiles(dir).take(limit)
    println(s"aggregating ${files.size} files")

    val analyzed = filesToStrings(files)
      .take(limit)
      .map(parse(_))
      .map(analyzeJson(_))
      //.map(identity(_))
      //.map(identity(_))
      // .foldLeft(MultiAggregate.empty) { (a, b) =>
      //   a aggregate analyzeJson(parse(b))
      // }
      .foldLeft(MultiAggregate.empty) { (a, b) =>
        a aggregate b
      }
      // .reduceLeft {(a, b) =>
      //   (a aggregate b)
      // }

    val prettyA = pretty(render(analyzed.toJson))
    val file = new File(out)
    val bw = new java.io.BufferedWriter(new java.io.FileWriter(file))
    bw.write(prettyA)
    bw.close()
  }
}

