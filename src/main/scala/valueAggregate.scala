import scala.collection.immutable.Map

/*
  TODO:
  !! - combine all json files!
  !! - add counts / occurrence percentages
  - NumberAggregate not yet implemented
  - switch to different json library
  - find cleaner merge implementation
 */

sealed trait ValueAggregate {
  type S <: ValueAggregate

  def aggregate[T <: ValueAggregate](other: T): ValueAggregate = {
    (this, other) match {
      case (a: NullAggregate, b: NullAggregate)     => a merge b
      case (a: BooleanAggregate, b: BooleanAggregate) => a merge b
      case (a: StringAggregate, b: StringAggregate) => a merge b
      case (a: ArrayAggregate, b: ArrayAggregate) => a merge b
      case (a: JsonObjectAggregate, b: JsonObjectAggregate) => a merge b
      case _ => throw new Error("multi aggregate time")
    }
  }

  def merge(other: S): S

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
//
case class JsonObjectAggregate(attributes: Map[String, ValueAggregate], count: Int = 1)
  extends ValueAggregate {

  type S = JsonObjectAggregate

  override def formatPretty(offset: Int = 0): String =
    this.attributes.mapValues({
      case jOA: JsonObjectAggregate => "\n" + (" " * (offset + 4)) + jOA.formatPretty(offset + 4)
      case aA: ArrayAggregate => aA.formatPretty(offset + 4)
      case x => x.formatPretty()
    }).mkString("\n" + (" " * offset))

  def merge(other: JsonObjectAggregate): JsonObjectAggregate = {
    val mergedAttrs = (this.attributes.keySet ++ other.attributes.keySet).map((k) =>
      (this.attributes.get(k), other.attributes.get(k)) match {
        case (Some(a), Some(b)) => (k, a aggregate b)
        case (Some(a), None) => (k, a)
        case (None, Some(b)) => (k, b)
      }
    ).toMap
    JsonObjectAggregate(mergedAttrs)
  }
}

object JsonObjectAggregate {
  def fromMap(jsonMap: Map[String, Any]): JsonObjectAggregate = {
    val m = jsonMap.map {
      case (k, v) => k -> ValueAggregate.makeAggregate(v)
    }

    JsonObjectAggregate(m)
  }
}

case class ArrayAggregate(values: MultiAggregate)
  extends ValueAggregate {

  type S = ArrayAggregate

  def merge(other: ArrayAggregate): ArrayAggregate = ArrayAggregate(this.values.merge(other.values))

  override def formatPretty(offset: Int = 0): String = "[\n " + values.formatPretty(offset + 4) + "\n" + (" " * offset) + "]"
}

object ArrayAggregate {
  def fromList(l: List[Any]): ValueAggregate = ArrayAggregate(MultiAggregate.fromList(l))
}

case class StringAggregate(values: Set[String], minLen: Int, maxLen: Int, avgLen: Int, count: Int)
  extends ValueAggregate {

  type S = StringAggregate

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
  type S = BooleanAggregate

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
  type S = NullAggregate

  def merge(other: NullAggregate): NullAggregate = NullAggregate(this.count + other.count)
}

object NullAggregate {
  def make: ValueAggregate = NullAggregate(1)
}

case class MultiAggregate(jsonAgg: Option[JsonObjectAggregate],
                          arrayAgg: Option[ArrayAggregate],
                          stringAgg: Option[StringAggregate],
                          booleanAgg: Option[BooleanAggregate],
                          nullAgg: Option[NullAggregate]
                         )
  extends ValueAggregate {

  type S = MultiAggregate

  override def formatPretty(offset: Int): String = {
    val presentTypes = List(jsonAgg, arrayAgg, stringAgg, booleanAgg, nullAgg).filter(_.isDefined)

    val prettyString = presentTypes match {
      case x :: Nil => x.get.formatPretty(offset)
      case x :: xs => x.get.formatPretty(offset) + "\n" + xs.map(_.get.formatPretty(offset))
      case List() => "Empty"
    }

    (" " * offset) + prettyString
  }

  def map2[T](a: Option[T], b: Option[T])(f:(T, T) => T): Option[T] =
    for {
      aVal <- a
      bVal <- b
    } yield { f(aVal, bVal) }

  def merge(other: MultiAggregate): MultiAggregate = {
    MultiAggregate(
      callOrReplaceWithEither(this.jsonAgg, other.jsonAgg)(_ merge _),
      callOrReplaceWithEither(this.arrayAgg, other.arrayAgg)(_ merge _),
      callOrReplaceWithEither(this.stringAgg, other.stringAgg)(_ merge _),
      callOrReplaceWithEither(this.booleanAgg, other.booleanAgg)(_ merge _),
      callOrReplaceWithEither(this.nullAgg, other.nullAgg)(_ merge _)
    )
  }

  override def aggregate[T <: ValueAggregate](other: T): MultiAggregate = {
    other match {
      case a: ArrayAggregate =>      this.copy(arrayAgg   = callOrReplace(this.arrayAgg, a)(_ merge _))
      case s: StringAggregate =>     this.copy(stringAgg  = callOrReplace(this.stringAgg, s)(_ merge _))
      case j: JsonObjectAggregate => this.copy(jsonAgg    = callOrReplace(this.jsonAgg, j)(_ merge _))
      case b: BooleanAggregate =>    this.copy(booleanAgg = callOrReplace(this.booleanAgg, b)(_ merge _))
      case n: NullAggregate =>       this.copy(nullAgg    = callOrReplace(this.nullAgg, n)(_ merge _))

      case err => throw new Error(s"implement aggregate for ${err}")
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
}

object MultiAggregate {
  def fromList(l: List[Any]): MultiAggregate = {
    l.foldLeft(MultiAggregate.empty)((mAgg, value) =>
      mAgg.aggregate(ValueAggregate.makeAggregate(value))
    )
  }

  def empty: MultiAggregate =
    MultiAggregate(
      None,
      None,
      None,
      None,
      None
    )
}



