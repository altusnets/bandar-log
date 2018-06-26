/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

/**
  * 3 Major base metrics
  *
  * ---IN-->[source]---OUT-->
  *
  * LAG = IN - OUT
  *
  */
object BaseMetrics {
  val IN = "IN"
  val OUT = "OUT"
  val LAG = "LAG"
}

object Metrics {

  /**
    * Realtime lag metric
    *
    * Difference between current timestamp and OUT metric
    *
    * REALTIME_LAG = System.currentTimeMillis() - OUT
    *
    */
  val REALTIME_LAG = "REALTIME_LAG"
}
