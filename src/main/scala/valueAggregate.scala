import scala.collection.immutable.Map

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
/*
  TODO:
  - Add counts to string value occurrences
  - what should MaxEnumSize be?
    - how can I tune it?
  - NumberAggregate not yet implemented
 */

sealed trait ValueAggregate {
  type S <: ValueAggregate

  val MaxEnumSize = 25

  val count: Int

  def aggregate[T <: ValueAggregate](other: T): ValueAggregate = {
    (this, other) match {
      case (a: NullAggregate,       b: NullAggregate)       => a merge b
      case (a: BooleanAggregate,    b: BooleanAggregate)    => a merge b
      case (a: StringAggregate,     b: StringAggregate)     => a merge b
      case (a: ArrayAggregate,      b: ArrayAggregate)      => a merge b
      case (a: JsonObjectAggregate, b: JsonObjectAggregate) => a merge b
      case (a: MultiAggregate,      b: MultiAggregate)      => a merge b
      case (a, b) => MultiAggregate.fromAggregateList(List(this, other))

      case _ => throw new Error("multi aggregate time")
    }
  }

  def merge(other: S): S

  def toJson: JObject
}

object ValueAggregate {
  def makeAggregate(value: JValue): ValueAggregate = {
    value match {
      case JNull           => NullAggregate.make
      case JBool(b)        => BooleanAggregate.fromBoolean(b)
      case JString(s)      => StringAggregate.fromString(s)
      case JArray(arr)     => ArrayAggregate.fromList(arr)
      case JObject(fields) => JsonObjectAggregate.fromMap(fields)

      case x          => throw new Error(x.getClass.toString)
    }
  }
}

case class JsonObjectAggregate(attributes: Map[String, ValueAggregate], count: Int)
  extends ValueAggregate {

  type S = JsonObjectAggregate

  def merge(other: JsonObjectAggregate): JsonObjectAggregate = {
    val mergedAttrs = (this.attributes.keySet ++ other.attributes.keySet).map((k) =>
      (this.attributes.get(k), other.attributes.get(k)) match {
        case (Some(a), Some(b)) => (k, a aggregate b)
        case (Some(a), None) => (k, a)
        case (None, Some(b)) => (k, b)
      }
    ).toMap
    JsonObjectAggregate(mergedAttrs, this.count + other.count)
  }

  def toJson: JObject = {
    ("type" -> "json") ~
      ("statistics" -> ("count" -> count)) ~
      ("attributes" ->
        attributes.foldLeft(JObject()) { (acc: JObject, kv: (String, ValueAggregate)) =>
          val (k, v) = kv
          acc ~ (k -> v.toJson)
        })
  }
}

object JsonObjectAggregate {
  // TODO: change name
  def fromMap(jsonMap: List[(String, JValue)]): JsonObjectAggregate = {
    val m = jsonMap.map {
      case (k, v) => k -> ValueAggregate.makeAggregate(v)
    }.toMap

    JsonObjectAggregate(m, 1)
  }
}

case class ArrayAggregate(values: MultiAggregate, count: Int)
  extends ValueAggregate {

  type S = ArrayAggregate

  def merge(other: ArrayAggregate): ArrayAggregate =
    ArrayAggregate(this.values.merge(other.values), this.count + other.count)

  def toJson: JObject =
    ("type" -> "array") ~
      ("statistics" -> ("count" -> count)) ~
      ("values" -> values.toJson)
}

object ArrayAggregate {
  def fromList(l: List[JValue]): ValueAggregate =
    ArrayAggregate(MultiAggregate.fromList(l), 1)
}

case class StringAggregate(values: Set[String], minLen: Int, maxLen: Int, avgLen: Int, overEnumLimit: Boolean, numBlank: Int, count: Int)
  extends ValueAggregate {

  type S = StringAggregate

  val MaxStringSize = 100

  def merge(other: StringAggregate): StringAggregate = {
   val enumLimitReached = this.overEnumLimit || other.overEnumLimit || (this.values.union(other.values).size > this.MaxEnumSize)

    StringAggregate(
      values = if (!enumLimitReached) this.values.union(other.values) else Set(),
      minLen = this.minLen min other.minLen,
      maxLen = this.maxLen max other.maxLen,
      avgLen = ((this.avgLen * this.count) + (other.avgLen * other.count)) / (this.count + other.count), // TODO: broken for 0 vals
      overEnumLimit = enumLimitReached,
      numBlank = this.numBlank + other.numBlank,
      count = this.count + other.count
    )
  }

  def toJson = {
    ("type" -> "string") ~
    ("values" -> values.map(StringAggregate.truncate(_, MaxStringSize))) ~
      ("statistics" ->
        ("minLen" -> minLen) ~
          ("maxLen" -> maxLen) ~
          ("avgLen" -> avgLen) ~
          ("numBlank" -> numBlank) ~
          ("overEnumLimit" -> overEnumLimit) ~
          ("count" -> count)
        )
  }
}

object StringAggregate {
  def fromString(s: String): StringAggregate =
    StringAggregate(
      values = Set(s),
      minLen = s.length,
      maxLen = s.length,
      avgLen = s.length,
      overEnumLimit = 25 < 1, // TODO: how do i read the trait val from here?
      numBlank = if (s.isEmpty) 1 else 0,
      count = 1
    )

  def truncate(s: String, n: Int) =
    if (s.length > n) {
      s.slice(0, n) + "..."
    } else {
      s
    }
}

case class BooleanAggregate(numTrue: Int, numFalse: Int, count: Int) extends ValueAggregate {
  type S = BooleanAggregate

  def merge(other: BooleanAggregate): BooleanAggregate =
    BooleanAggregate(
      this.numTrue + other.numTrue,
      this.numFalse + other.numFalse,
      this.count + other.count
    )

  def toJson =
    ("type" -> "boolean") ~
      ("statistics" ->
        ("numTrue" -> numTrue) ~
          ("numFalse" -> numFalse) ~
          ("count" -> count))
}

object BooleanAggregate {
  def fromBoolean(b: Boolean) = b match {
    case true => BooleanAggregate(1, 0, 1)
    case false => BooleanAggregate(0, 1, 1)
  }
}

case class NullAggregate(count: Int) extends ValueAggregate {
  type S = NullAggregate

  def merge(other: NullAggregate): NullAggregate = NullAggregate(this.count + other.count)

  def toJson =
    ("type" -> "null") ~
      ("statistics" ->
        ("count" -> count))
}

object NullAggregate {
  def make: ValueAggregate = NullAggregate(1)
}

case class MultiAggregate(jsonAgg: Option[JsonObjectAggregate],
                          arrayAgg: Option[ArrayAggregate],
                          stringAgg: Option[StringAggregate],
                          booleanAgg: Option[BooleanAggregate],
                          nullAgg: Option[NullAggregate],
                          count: Int
                         )
  extends ValueAggregate {

  type S = MultiAggregate

  def merge(other: MultiAggregate): MultiAggregate = {
    MultiAggregate(
      callOrReplaceWithEither(this.jsonAgg, other.jsonAgg)(_ merge _),
      callOrReplaceWithEither(this.arrayAgg, other.arrayAgg)(_ merge _),
      callOrReplaceWithEither(this.stringAgg, other.stringAgg)(_ merge _),
      callOrReplaceWithEither(this.booleanAgg, other.booleanAgg)(_ merge _),
      callOrReplaceWithEither(this.nullAgg, other.nullAgg)(_ merge _),
      this.count + other.count
    )
  }

  override def aggregate[T <: ValueAggregate](other: T): MultiAggregate = {
    other match {
      case a: ArrayAggregate =>      this.copy(arrayAgg   = callOrReplace(this.arrayAgg, a)(_ merge _), count = count + 1)
      case s: StringAggregate =>     this.copy(stringAgg  = callOrReplace(this.stringAgg, s)(_ merge _), count = count + 1)
      case j: JsonObjectAggregate => this.copy(jsonAgg    = callOrReplace(this.jsonAgg, j)(_ merge _), count = count + 1)
      case b: BooleanAggregate =>    this.copy(booleanAgg = callOrReplace(this.booleanAgg, b)(_ merge _), count = count + 1)
      case n: NullAggregate =>       this.copy(nullAgg    = callOrReplace(this.nullAgg, n)(_ merge _), count = count + 1)
      case m: MultiAggregate =>      this merge m
    }
  }

  def callOrReplaceWithEither[T](thisVal: Option[T], otherVal: Option[T])(f: (T, T) => T): Option[T] = {
    (thisVal, otherVal) match {
      case (Some(v), Some(oV)) => Some(f(v, oV))
      case (Some(v), None) => Some(v)
      case (None, Some(oV)) => Some(oV)
      case (None, None) => None
    }
  }

  def callOrReplace[T](thisVal: Option[T], otherVal: T)(f:(T, T) => T): Option[T] = {
    thisVal match {
      case Some(v) => Some(f(v, otherVal))
      case None => Some(otherVal)
    }
  }

  def toJson = {
    ("type" -> "multiple_types") ~
      ("count" -> count) ~
      ("json" -> jsonAgg.map(_.toJson)) ~
      ("array" -> arrayAgg.map(_.toJson)) ~
      ("string" -> stringAgg.map(_.toJson)) ~
      ("boolean" -> booleanAgg.map(_.toJson)) ~
      ("null" -> nullAgg.map(_.toJson))
  }
}

object MultiAggregate {
  def fromList(l: List[JValue]): MultiAggregate = {
    l.foldLeft(MultiAggregate.empty)((mAgg, value) =>
      mAgg.aggregate(ValueAggregate.makeAggregate(value))
    )
  }

  def fromAggregateList(l: List[ValueAggregate]): MultiAggregate = {
    l.foldLeft(MultiAggregate.empty)((mAgg, valueAggregate) =>
      valueAggregate match {
        case m: MultiAggregate => mAgg merge m
        case o => mAgg.aggregate(o)
      }
    )
  }

  def empty: MultiAggregate =
    MultiAggregate(
      None,
      None,
      None,
      None,
      None,
      0
    )
}

