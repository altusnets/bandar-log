package com.aol.one.dwh.infra.util

import org.scalatest.FunSuite

class ExceptionPrinterTest extends FunSuite {

  test("getting stack trace as string") {
    val exceptionMessage = "My test exception"
    val e = new Exception(exceptionMessage)
    object ExceptionPrinterImpl extends ExceptionPrinter {}
    val stackTraceString = new ExceptionPrinterImpl.ExceptionOpts(e).getStringStackTrace

    assert(stackTraceString.contains(exceptionMessage), "Exception print stack should contain message")
  }

}
