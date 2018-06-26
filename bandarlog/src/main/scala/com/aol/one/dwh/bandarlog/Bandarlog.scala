/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.bandarlog.metrics.MetricProvider
import com.aol.one.dwh.bandarlog.reporters.MetricReporter
import com.aol.one.dwh.bandarlog.scheduler.Scheduler
import com.aol.one.dwh.infra.util.{ExceptionPrinter, LogTrait}

import scala.util.Try
import scala.util.control.NonFatal

/**
  * Bandarlog
  *
  * The main component which is responsible for the managing data providers and reporters
  * [data source] --> [connector] --> [provider] --> [BANDARLOG] --> [reporter] --> [monitor]
  */
class Bandarlog[V](
    providers: Seq[MetricProvider[V]],
    reporters: Seq[MetricReporter],
    scheduler: Scheduler
  ) extends LogTrait with ExceptionPrinter {

  def execute(): Unit = {
    scheduler.schedule(() =>
      providers.foreach { metricProvider =>
        val metric = metricProvider.metric
        val provider = metricProvider.provider

        Try {
          val value = provider.provide()
          metric.value.setValue(value.getValue)
        }.recover {
          case NonFatal(e) =>
            logger.error("Catching exception {}", e.getStringStackTrace)
            metric.value.setValue(None)
        }

        logger.info(s"Metric:[${metric.prefix}.${metric.name}] Tags:[${metric.tags.mkString(",")}] Value:[${metric.value.getValue}]")
      })

    reporters.foreach(_.start())
  }

  def shutdown(): Unit = {
    reporters.foreach(_.stop())
    scheduler.shutdown()
  }
}
