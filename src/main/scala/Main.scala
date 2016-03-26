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
  def analyzeJson(json: Option[Any]): Either[String, ValueAggregate] =
    json match {
      case Some(x) => x match {
        case l: List[Any] => ArrayAggregate.fromList(l)
        case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)
      }
      case None => Left("parser returned none")
    }

  def parseJson(body: String): Option[Any] = {
    import scala.util.parsing.json._

    JSON.parseFull(body)
  }

  def formatAnalysis(analysis: Either[String, ValueAggregate]) = {
    analysis match {
      case Right(agg) => agg.formatPretty()
      case Left(err) => "Error: " + err
    }
  }

  def main(args: Array[String]) = {
    println("input:")
    println(args.mkString("; ") + "\n") // /Users/victor/code/little-brother/114th_bulk_data/bills

    val dir = args(0)
    val files = getListOfFiles(dir)
    println(s"found ${files.length} files\n")

    val f = files(0)
    println(s"first file: ${f}\n")

    // read file body into string
    val source = io.Source.fromFile(f)
    val body = try source.mkString finally source.close

    // parse file body json
    val parsed = parseJson(body)

    // analyze parsed json
    val analysis = analyzeJson(parsed)
    val prettyA = formatAnalysis(analysis)
    println(prettyA)
  }
}

