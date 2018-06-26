/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import java.net.InetAddress
import java.util.concurrent.TimeUnit

import com.aol.one.dwh.infra.config.{DatadogConfig, ReportConfig}
import com.codahale.metrics.MetricRegistry
import org.coursera.metrics.datadog.DatadogReporter
import org.coursera.metrics.datadog.transport.UdpTransport

import scala.collection.JavaConverters._

/**
  * Datadog Metric Reporter
  */
class DatadogMetricReporter(config: DatadogConfig, tags: List[String], metricRegistry: MetricRegistry, reportConf: ReportConfig)
  extends MetricReporter {

  private lazy val datadogReporter = {
    val udpTransport = new UdpTransport.Builder().build()

    DatadogReporter.
      forRegistry(metricRegistry).
      withTags(tags.asJava).
      withTransport(udpTransport).
      withHost(config.host.getOrElse(InetAddress.getLocalHost.getHostName)).
      build()
  }

  override def start(): Unit = {
    datadogReporter.start(reportConf.interval, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    datadogReporter.stop()
  }

}
