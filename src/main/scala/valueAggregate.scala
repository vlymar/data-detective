import scala.collection.Map

/*
  TODO:
  - NumberAggregate not yet implemented
  - no merging implemented yet
 */

sealed trait ValueAggregate {
  type T <: ValueAggregate

  val count: Int

  def formatPretty(offset: Int = 0) = this.toString

  def merge(other: T): T
}

object ValueAggregate {
  def makeAggregate(value: Any): Either[String, ValueAggregate] = {
    value match {
      case null                => Right(NullAggregate.make)
      case b: Boolean          => Right(BooleanAggregate.fromBoolean(b))
      case s: String           => Right(StringAggregate.fromString(s))
      case l: List[Any]        => ArrayAggregate.fromList(l)
      case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)

      case x                   => Left(x.getClass.toString)
    }
  }
}

case class JsonObjectAggregate(attributes: Map[String, Either[String, ValueAggregate]],
                               count: Int = 1)
  extends ValueAggregate {

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

case class ArrayAggregate(values: Set[Either[String, ValueAggregate]],
                          minLen: Int, maxLen: Int, avgLen: Int, count: Int)
  extends ValueAggregate {

  type T = ArrayAggregate

  def merge(other: ArrayAggregate): ArrayAggregate = this // TODO
}

object ArrayAggregate {
  // TODO: first solid merging should be done here
  def fromList(l: List[Any]): Either[String, ValueAggregate] =
    Right(ArrayAggregate(Set(), l.length, l.length, l.length, 1))
}

case class StringAggregate(values: Set[String], minLen: Int, maxLen: Int, avgLen: Int, count: Int)
  extends ValueAggregate {

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

