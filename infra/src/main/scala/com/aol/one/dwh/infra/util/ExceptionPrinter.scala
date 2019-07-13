/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.util

import java.io.{PrintWriter, StringWriter}

import resource._

trait ExceptionPrinter {

  implicit class ExceptionOpts(e: Throwable) {
    def getStringStackTrace: String = {
      val sw = new StringWriter()
      managed(new PrintWriter(sw)) apply e.printStackTrace
      sw.toString()
    }
  }

}
