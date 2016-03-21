import scala.collection.immutable.HashMap

object Main {
  import scala.collection.Map
  import java.io.File


  case class JsonObjectAggregate(attributes: Map[String, ValueAggregate], count: Int = 1) extends ValueAggregate {
    def mkString: String = this.attributes.mkString("\n")
  }

  object JsonObjectAggregate {
    def fromMap(jsonMap: Map[String, Any]): JsonObjectAggregate = {
      val m = jsonMap.map {
        case (k, v) => k -> ValueAggregate.makeAggregate(v)
      }
      JsonObjectAggregate(m)
    }

    def mergeAggregates(agg1: JsonObjectAggregate, agg2: JsonObjectAggregate): JsonObjectAggregate = {
      // TODO: implement for real
      agg1
    }
  }

  sealed trait ValueAggregate {
    val count: Int
  }

  object ValueAggregate {
    def makeAggregate[A](value: A): ValueAggregate = {
      value match {
        case null => NullAggregate(1)
        case s: String => StringAggregate(s.length, s.length, s.length, 1)
        case b: Boolean => BooleanAggregate.fromBoolean(b)
        case l: List[Any] => ArrayAggregate(l.length, l.length, l.length, 1)
        case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)

        case x => {println(s"${x.getClass.toString}"); NullAggregate(1)}
      }
    }
  }

  case class StringAggregate(minLen: Int, maxLen: Int, avgLen: Int, count: Int) extends ValueAggregate
  case class NumberAggregate(min: Int, max: Int, avg: Int, count: Int) extends ValueAggregate
  case class BooleanAggregate(numTrue: Int, numFalse: Int, count: Int) extends ValueAggregate
  case class ArrayAggregate(minLen: Int, maxLen: Int, avgLen: Int, count: Int) extends ValueAggregate
  case class NullAggregate(count: Int) extends ValueAggregate

  object BooleanAggregate {
    def fromBoolean(b: Boolean) = b match {
      case true => BooleanAggregate(1, 0, 1)
      case false => BooleanAggregate(0, 1, 1)
    }
  }

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

  def analyzeJsonObjects(objects: List[Map[String, Any]]): JsonObjectAggregate = {
    objects.map(JsonObjectAggregate.fromMap(_)).reduce(JsonObjectAggregate.mergeAggregates)
  }

  def analyzeJson(json: Option[Any]): Either[String, String] =
    json match {
      case Some(x) => x match {
        case l: List[Any] => Right(analyzeJsonArray(l))
        case m: Map[String, Any] => Right(JsonObjectAggregate.fromMap(m).mkString)
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
    printAnalysis(analysis)
  }
}

