package flat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.joda.time.{LocalDate, LocalTime}
import shapeless._
import shapeless.labelled.FieldType

trait Flat[A] extends Serializable {
  def row(a: A): Row
  def schema: StructType
}

object Flat {

  def apply[A](implicit f: Flat[A]): Flat[A] = f

  def prepend(a: String, b: String, sep: String = "."): String = {
    if (b.isEmpty) a else s"$a$sep$b"
  }

  def toDF[A](s: SparkSession, rdd: RDD[A])(implicit f: Flat[A]): DataFrame = {
    s.createDataFrame(rdd.map(f.row), f.schema)
  }

  implicit val flatInt: Flat[Int] = new Flat[Int] {
    override def row(a: Int): Row = Row(a)
    override def schema: StructType = StructType(Seq(StructField("", IntegerType, nullable = false)))
  }

  implicit val flatString: Flat[String] = new Flat[String] {
    override def row(a: String): Row = Row(a)
    override def schema: StructType = StructType(Seq(StructField("", StringType, nullable = false)))
  }

  implicit val flatRowLocalDate: Flat[LocalDate] = new Flat[LocalDate] {
    override def row(t: LocalDate): Row = Row(new java.sql.Date(t.toDate.getTime))
    override def schema: StructType = StructType(Seq(StructField("", DateType, nullable = false)))
  }

  implicit val flatRowLocalTime: Flat[LocalTime] = new Flat[LocalTime] {
    override def row(t: LocalTime): Row = Row(t.toString)
    override def schema: StructType = StructType(Seq(StructField("", StringType, nullable = false)))
  }

  implicit def flatOption[A](implicit fa: Flat[A]): Flat[Option[A]] = new Flat[Option[A]] {
    override def row(a: Option[A]): Row = a.fold(Row.fromSeq(Seq.fill(fa.schema.fields.length)(null)))(fa.row)
    override def schema: StructType = {
      val s = fa.schema
      s.copy(fields = s.fields.map(_.copy(nullable = true)))
    }
  }

  implicit val flatHNil: Flat[HNil] = new Flat[HNil] {
    override def row(a: HNil): Row = Row()
    override def schema: StructType = StructType(Seq())
  }

  implicit def flatHList[K <: Symbol, H, T <: HList](implicit witness: Witness.Aux[K], hi: Lazy[Flat[H]], ti: Flat[T]): Flat[FieldType[K, H] :: T] =
    new Flat[FieldType[K, H] :: T] {
      override def row(a: FieldType[K, H] :: T): Row = a match {
        case h :: t => Row.merge(hi.value.row(h), ti.row(t))
      }

      override def schema: StructType = {
        val fieldName: String = witness.value.name
        val h: Array[StructField] = hi.value.schema.fields.map(f => f.copy(name = prepend(fieldName, f.name)))
        val t: Array[StructField] = ti.schema.fields
        StructType(h ++ t)
      }
  }

  implicit def flatGeneric[A, R] ( implicit generic: LabelledGeneric.Aux[A, R], flat: Lazy[Flat[R]] ): Flat[A] = new Flat[A] {
    override def row(a: A): Row = flat.value.row(generic.to(a))
    override def schema: StructType = flat.value.schema
  }
}

