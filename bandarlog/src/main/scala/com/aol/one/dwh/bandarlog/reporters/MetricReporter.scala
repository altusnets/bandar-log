/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import com.aol.one.dwh.infra.config.{ReportConfig, ReporterConfig, Tag}
import com.aol.one.dwh.infra.config.RichConfig._
import com.codahale.metrics.MetricRegistry
import com.typesafe.config.Config
import TagsFormatter.datadogFormat

/**
  * Base trait for all Metric Reporters
  */
trait MetricReporter {

  def start(): Unit

  def stop(): Unit
}

object MetricReporter {

  def apply(reporter: ReporterConfig, tags: List[Tag], metricRegistry: MetricRegistry, mainConf: Config, reportConf: ReportConfig): MetricReporter = {
    reporter.reporterType match {
      case "datadog" =>
        val datadogTags = TagsFormatter.format(tags, datadogFormat)
        val datadogConfig = mainConf.getDatadogConfig(reporter.configId)
        new DatadogMetricReporter(datadogConfig, datadogTags, metricRegistry, reportConf)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported reporter:[$reporter]")
    }
  }
}
