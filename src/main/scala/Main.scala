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

  sealed trait ValueAggregate {
    val count: Int

    def formatPretty(offset: Int = 0) = this.toString
  }

  object ValueAggregate {
    def makeAggregate[A](value: A): Either[String, ValueAggregate] = {
      value match {
        case null         => Right(NullAggregate(1))
        case s: String    => Right(StringAggregate(s.length, s.length, s.length, 1))
        case b: Boolean   => Right(BooleanAggregate.fromBoolean(b))
        case l: List[Any] => Right(ArrayAggregate(l.length, l.length, l.length, 1))
        case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)

        case x => Left(x.getClass.toString) // TODO: these arent getting printed with error prefixes (but are being pretty printed)
      }
    }
  }

  case class JsonObjectAggregate(attributes: Map[String, Either[String, ValueAggregate]], count: Int = 1) extends ValueAggregate {
    override def formatPretty(offset: Int = 0): String =
      this.attributes.mapValues({
        case Right(v) =>  v match {
          case jOA: JsonObjectAggregate => jOA.formatPretty(offset + 4)
          case x => x.formatPretty()
        }
        case Left(err) => err
      }).mkString("\n" + (" " * offset))
  }

  object JsonObjectAggregate {
    def fromMap(jsonMap: Map[String, Any]): Either[String, JsonObjectAggregate] = {
      val m = jsonMap.map {
        case (k, v) => k -> ValueAggregate.makeAggregate(v)
      }
      Right(JsonObjectAggregate(m))
    }

    def mergeAggregates(agg1: Either[String, JsonObjectAggregate],
                        agg2: Either[String, JsonObjectAggregate]): Either[String, JsonObjectAggregate] = {
      // TODO: implement for real
      Left("not implemented yet")
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

  def parseJson(body: String): Option[Any] = {
    import scala.util.parsing.json._

    JSON.parseFull(body)
  }

  def analyzeJsonArray(arr: List[Any]): String =
    "array parsing not yet implemented"

  def analyzeJsonObjects(objects: List[Map[String, Any]]): Either[String, JsonObjectAggregate] = {
    objects
      .map(JsonObjectAggregate.fromMap(_))
      .reduce(JsonObjectAggregate.mergeAggregates)
  }

  def analyzeJson(json: Option[Any]): Either[String, List[JsonObjectAggregate]] =
    json match {
      case Some(x) => x match {
        case l: List[Any] => Left(analyzeJsonArray(l))
        case m: Map[String, Any] => JsonObjectAggregate.fromMap(m) match {
          case Right(x) => Right(List(x))
          case Left(y) => Left(y)
        }
      }
      case None => Left("parser returned none")
    }

  def formatAnalysis(analysis: Either[String, List[JsonObjectAggregate]]) = {
    analysis match {
      case Right(l) => l.map(_.formatPretty()).mkString("\n ---- \n")
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

