package flat

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import shapeless._

trait Flat[A] {
  def row(a: A): Row
  def schema: StructType
}

object Flat {

  def apply[A](implicit f: Flat[A]): Flat[A] = f

  implicit val flatInt: Flat[Int] = new Flat[Int] {
    override def row(a: Int): Row = Row(a)
    override def schema: StructType = StructType(Seq(StructField("", IntegerType, nullable = false)))
  }

  implicit val flatString: Flat[String] = new Flat[String] {
    override def row(a: String): Row = Row(a)
    override def schema: StructType = StructType(Seq(StructField("", StringType, nullable = false)))
  }

  implicit def flatOption[A](implicit fa: Flat[A]): Flat[Option[A]] = new Flat[Option[A]] {
    override def row(a: Option[A]): Row = a.fold(Row(Seq.fill(fa.schema.fields.length)(null): _*))(a => fa.row(a))
    override def schema: StructType = {
        val s = fa.schema
        s.copy(fields = s.fields.map(_.copy(nullable = true)))
      }
  }

  implicit val flatHNil: Flat[HNil] = new Flat[HNil] {
    override def row(a: HNil): Row = Row()
    override def schema: StructType = StructType(Seq())
  }

  implicit def flatHList[H, T <: HList](implicit hi: Lazy[Flat[H]], ti: Flat[T]): Flat[H :: T] = new Flat[H :: T] {
    override def row(a: H :: T): Row = a match {
      case h :: t => Row.merge(hi.value.row(h), ti.row(t))
    }
    override def schema: StructType = {
      val h: Array[StructField] = hi.value.schema.fields
      val t: Array[StructField] = ti.schema.fields
      StructType(h ++ t)
    }
  }

  implicit def flatGeneric[A, R] ( implicit generic: Generic.Aux[A, R], flat: Lazy[Flat[R]] ): Flat[A] = new Flat[A] {
    override def row(a: A): Row = flat.value.row(generic.to(a))
    override def schema: StructType = flat.value.schema
  }
}
