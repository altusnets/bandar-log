/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.reporters

import com.aol.one.dwh.bandarlog.metrics.Metric
import com.aol.one.dwh.infra.util.LogTrait
import com.codahale.metrics.{Gauge, MetricRegistry}


/**
  * Registry Creator
  *
  * Creates metric registry with/without registered metrics
  */
object RegistryFactory extends LogTrait {

  def create(): MetricRegistry = {
    new MetricRegistry()
  }

  def createWithMetric[V](metric: Metric[V]): MetricRegistry = {
    val metricRegistry = create()
    metricRegistry.register(s"${metric.prefix}.${metric.name}", toGauge(metric))
    metricRegistry
  }

  private def toGauge[V](metric: Metric[V]): Gauge[V] = {
    new Gauge[V] {
      override def getValue: V = {
        // null values will be filtered and not reported
        metric.value.getValue.getOrElse(None.orNull.asInstanceOf[V])
      }
    }
  }

}
