object Main {

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
  def analyzeJson(json: Option[Any]): ValueAggregate =
    json match {
      case Some(x) => ValueAggregate.makeAggregate(x)
      case None => throw new Error("parser returned none")
    }

  def parseJson(body: String): Option[Any] = {
    import scala.util.parsing.json._

    JSON.parseFull(body)
  }

  def formatAnalysis(analysis: ValueAggregate) = {
      analysis.formatPretty()
  }

  def filesToStrings(files: List[File]): List[String] = {
    files.map { (f) =>
      val source = io.Source.fromFile(f)
      try source.mkString finally source.close
    }
  }

  def main(args: Array[String]) = {
    println("input:")
    println(args.mkString("; ") + "\n") // /Users/victor/code/little-brother/114th_bulk_data/bills

    val dir = args(0)
    val files = getListOfFiles(dir)
    println(s"found ${files.length} files\n")

    val someFiles: List[File] = files.take(8000)
    //println(someFiles.mkString("\n"))


    val tStrings: List[String] = filesToStrings(someFiles)
    val tParsed: List[Option[Any]] = tStrings.map { parseJson(_) }

    val analyzed = tParsed.map(analyzeJson _).reduce(_ aggregate _)

    val prettyA = formatAnalysis(analyzed)
    println(prettyA)
  }
}

