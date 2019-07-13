package com.aol.one.dwh.infra.util

import org.scalatest.FunSuite

class ExceptionPrinterTest extends FunSuite {

  test("getting stack trace as string") {
    val e = new Exception("sf") with ExceptionPrinter
    val st = List(
      new StackTraceElement("f.q.d.n.Class", "c", "Class.java", 30),
      new StackTraceElement("f.q.d.n.Class", "b", "Class.java", 20),
      new StackTraceElement("f.q.d.n.Class", "a", "Class.java", 10)
    )
    e.setStackTrace(st.toArray)

    val expected: String = st.map(_.toString()).mkString("\n") ++ "\n"
    val actual: String = e.getStackTraceString

    assert(expected == actual, "Stacktrace converted to string correctly")
  }

}
