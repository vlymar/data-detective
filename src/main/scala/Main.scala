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

  def parseJson(body: String): Option[Any] = {
    import scala.util.parsing.json._

    JSON.parseFull(body)
  }

  def analyzeJsonArray(arr: List[Any]): String =
    "array parsing not yet implemented"

  def analyzeJsonObject(obj: Map[String, Any]): String =
    obj.mkString("\n")

  def analyzeJson(json: Option[Any]): Either[String, String] =
    json match {
      case Some(x) => x match {
        case l: List[Any] => Right(analyzeJsonArray(l))
        case m: Map[String, Any] => Right(analyzeJsonObject(m))
      }
      case None => Left("parser returned none")
  }

  def printAnalysis(analysis: Either[String, String]) = {
    val out = analysis match {
      case Right(info) => info
      case Left(err) => "Error: " + err
    }
    println(out)
  }

  /*
     /Users/victor/code/little-brother/114th_bulk_data/bills
   */
  def main(args: Array[String]) = {
    println("input:")
    println(args.mkString("; ") + "\n")

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
    printAnalysis(analysis)
  }
}

