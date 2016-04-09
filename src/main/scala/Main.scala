object Main {

  import org.json4s._
  import org.json4s.JsonDSL._
  import org.json4s.jackson.JsonMethods._
  import scala.collection.Map
  import java.io.File

  def analyzeJson(json: JValue): ValueAggregate =
    json match {
      case JArray(arr)     => ArrayAggregate.fromList(arr)
      case JObject(fields) => JsonObjectAggregate.fromFields(fields)
    }

  def aggregateStream(s: => Stream[String]): ValueAggregate =
    s.map(parse(_))
     .map(analyzeJson(_))
     .foldLeft(MultiAggregate.empty) { (a, b) =>
       a aggregate b
     }

  def main(args: Array[String]) = {
    println("input:")

    // expected input: files output_file limit
    println(args.mkString("; ") + "\n")

    val dir = args(0)
    val out = args(1)

    val files: List[File] = FileUtil.getListOfFiles(dir)

    println(s"aggregating ${files.size} files")

    val analyzed = aggregateStream(FileUtil.fileStream(files))
    val prettyA = pretty(render(analyzed.toJson))
    FileUtil.writeFile(out, prettyA)
  }
}

