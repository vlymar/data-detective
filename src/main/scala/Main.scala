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
    type T <: ValueAggregate

    val count: Int

    def formatPretty(offset: Int = 0) = this.toString

    def merge(other: T): T
  }

  object ValueAggregate {
    def makeAggregate(value: Any): Either[String, ValueAggregate] = {
      // TODO: numberaggregate. does my current parser even return num values?
      value match {
        case null => Right(NullAggregate.make)
        case b: Boolean => Right(BooleanAggregate.fromBoolean(b))
        case s: String => Right(StringAggregate.fromString(s))
        case l: List[Any]        => ArrayAggregate.fromList(l)
        case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)

        case x => Left(x.getClass.toString) // TODO: these arent getting printed with error prefixes (but are being pretty printed)
      }
    }
  }

  case class JsonObjectAggregate(attributes: Map[String, Either[String, ValueAggregate]], count: Int = 1) extends ValueAggregate {
    type T = JsonObjectAggregate

    override def formatPretty(offset: Int = 0): String =
      this.attributes.mapValues({
        case Right(v) =>  v match {
          case jOA: JsonObjectAggregate => jOA.formatPretty(offset + 4)
          case x => x.formatPretty()
        }
        case Left(err) => err
      }).mkString("\n" + (" " * offset))

    def merge(other: JsonObjectAggregate): JsonObjectAggregate =
      this // TODO
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

  case class ArrayAggregate(values: Set[Either[String, ValueAggregate]], minLen: Int, maxLen: Int, avgLen: Int, count: Int) extends ValueAggregate {
    type T = ArrayAggregate

    def merge(other: ArrayAggregate): ArrayAggregate = this
  }

  object ArrayAggregate {
    // TODO: first solid merging should be done here
    def fromList(l: List[Any]): Either[String, ValueAggregate] =
      Right(ArrayAggregate(Set(), l.length, l.length, l.length, 1))
  }

  case class StringAggregate(values: Set[String], minLen: Int, maxLen: Int, avgLen: Int, count: Int) extends ValueAggregate {
    type T = StringAggregate

    override def formatPretty(offset: Int): String =
      "StringAggregate{ " + values.map("\"" + _ + "\"").mkString(", ") + " }"

    def merge(other: StringAggregate): StringAggregate = this // TODO
  }

  object StringAggregate {
    def fromString(s: String): StringAggregate =
      StringAggregate(Set(s), s.length, s.length, s.length, 1)
  }

  case class BooleanAggregate(numTrue: Int, numFalse: Int, count: Int) extends ValueAggregate {
    type T = BooleanAggregate

    def merge(other: BooleanAggregate): BooleanAggregate = this // TODO
  }

  object BooleanAggregate {
    def fromBoolean(b: Boolean) = b match {
      case true => BooleanAggregate(1, 0, 1)
      case false => BooleanAggregate(0, 1, 1)
    }
  }

  case class NullAggregate(count: Int) extends ValueAggregate {
    type T = NullAggregate

    def merge(other: NullAggregate): NullAggregate = this // TODO
  }

  object NullAggregate {
    def make: ValueAggregate = NullAggregate(1)
  }

  def parseJson(body: String): Option[Any] = {
    import scala.util.parsing.json._

    JSON.parseFull(body)
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

