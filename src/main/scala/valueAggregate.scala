import scala.collection.Map

/*
  TODO:
  - NumberAggregate not yet implemented
  - no merging implemented yet
  - switch to different json library
 */

sealed trait ValueAggregate {
  type T <: ValueAggregate

  val count: Int

  def merge(other: T): T

  def formatPretty(offset: Int = 0) = this.toString
}

object ValueAggregate {
  def makeAggregate(value: Any): ValueAggregate = {
    value match {
      case null                => NullAggregate.make
      case b: Boolean          => BooleanAggregate.fromBoolean(b)
      case s: String           => StringAggregate.fromString(s)
      case l: List[Any]        => ArrayAggregate.fromList(l)
      case m: Map[String, Any] => JsonObjectAggregate.fromMap(m)

      case x                   => throw new Error(x.getClass.toString)
    }
  }
}

case class JsonObjectAggregate(attributes: Map[String, ValueAggregate], count: Int = 1)
  extends ValueAggregate {

  type T = JsonObjectAggregate

  override def formatPretty(offset: Int = 0): String =
    this.attributes.mapValues({
      case jOA: JsonObjectAggregate => jOA.formatPretty(offset + 4)
      case x => x.formatPretty()
    }).mkString("\n" + (" " * offset))

  def merge(other: JsonObjectAggregate): JsonObjectAggregate =
    this // TODO -> merge attribute hashes by key, update count
}

object JsonObjectAggregate {
  def fromMap(jsonMap: Map[String, Any]): JsonObjectAggregate = {
    val m = jsonMap.map {
      case (k, v) => k -> ValueAggregate.makeAggregate(v)
    }

    JsonObjectAggregate(m)
  }
}

case class ArrayAggregate(values: Set[ValueAggregate], minLen: Int, maxLen: Int, avgLen: Int, count: Int)
  extends ValueAggregate {

  type T = ArrayAggregate

  def merge(other: ArrayAggregate): ArrayAggregate = this // TODO -> merge the value sets, update stats
}

object ArrayAggregate {
  // TODO: how do i do this without explicit type matching??
  def fromList(l: List[Any]): ArrayAggregate = {
    val aggregatesByClass = l.map(ValueAggregate.makeAggregate(_)).groupBy(_.getClass)

    val aggregates = aggregatesByClass.mapValues { (aggregateList) =>
      aggregateList.reduce {(agg1: ValueAggregate, agg2: ValueAggregate) =>
        (agg1, agg2) match {
          case (a: JsonObjectAggregate, b: JsonObjectAggregate) => a.merge(b)
          case (a: ArrayAggregate, b: ArrayAggregate) => a.merge(b)
          case (a: StringAggregate, b: StringAggregate) => a.merge(b)
          case (a: BooleanAggregate, b: BooleanAggregate) => a.merge(b)
          case (a: NullAggregate, b: NullAggregate) => a.merge(b)
        }
      }
    }.values.toSet

    ArrayAggregate(aggregates, l.length, l.length, l.length, 1)
  }
}

case class StringAggregate(values: Set[String], minLen: Int, maxLen: Int, avgLen: Int, count: Int)
  extends ValueAggregate {

  type T = StringAggregate

  override def formatPretty(offset: Int): String =
    "StringAggregate{ " + values.map("\"" + _ + "\"").mkString(", ") + " }"

  def merge(other: StringAggregate): StringAggregate =
    StringAggregate(
      this.values.union(other.values),
      this.minLen min other.minLen,
      this.maxLen max other.maxLen,
      ((this.avgLen * this.count) + (other.avgLen * other.count)) / (this.count + other.count),
      this.count + other.count
    )

}

object StringAggregate {
  def fromString(s: String): StringAggregate =
    StringAggregate(Set(s), s.length, s.length, s.length, 1)
}

case class BooleanAggregate(numTrue: Int, numFalse: Int, count: Int) extends ValueAggregate {
  type T = BooleanAggregate

  def merge(other: BooleanAggregate): BooleanAggregate =
    BooleanAggregate(
      this.numTrue + other.numTrue,
      this.numFalse + other.numFalse,
      this.count + other.count
    )
}

object BooleanAggregate {
  def fromBoolean(b: Boolean) = b match {
    case true => BooleanAggregate(1, 0, 1)
    case false => BooleanAggregate(0, 1, 1)
  }
}

case class NullAggregate(count: Int) extends ValueAggregate {
  type T = NullAggregate

  def merge(other: NullAggregate): NullAggregate = NullAggregate(this.count + other.count)
}

object NullAggregate {
  def make: ValueAggregate = NullAggregate(1)
}

