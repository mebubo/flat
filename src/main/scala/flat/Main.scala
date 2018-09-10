package flat

case class A(a: Option[B])
case class B(b: (String, String, Option[Int]))

case class A1(a: B1)
case class B1(b: (String, String))

object Main {
  def printSchema[A](a: A)(implicit f: Flat[A]): Unit = println(f.schema)
  def printRow[A](a: A)(implicit f: Flat[A]): Unit = println(f.row(a))

  def main(args: Array[String]): Unit = {
    val a = (Some(2), Some("foo"), None)
    printSchema(a)
    printRow(a)
    val b: A = A(Some(B(("foo", "bar", Some(1)))))
    printSchema(b)
    printRow(b)
    val b1: A1 = A1(B1(("foo", "baz")))
    printSchema(b1)
    printRow(b1)
  }
}

