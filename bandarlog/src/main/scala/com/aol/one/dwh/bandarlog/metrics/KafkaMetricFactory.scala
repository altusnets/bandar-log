/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.connectors.KafkaConnector
import com.aol.one.dwh.bandarlog.metrics.BaseMetrics.{IN, LAG, OUT}
import com.aol.one.dwh.bandarlog.providers.{KafkaInMessagesProvider, KafkaLagProvider, KafkaOutMessagesProvider}
import com.aol.one.dwh.infra.config.{Tag, Topic}

/**
  * Kafka Metric Factory
  *
  * Provides pair Metric -> Provider by metric id
  */
class KafkaMetricFactory(kafkaConnector: KafkaConnector) {

  def create(metricId: String, metricPrefix: String, topic: Topic): MetricProvider[Long] = {

    val tags = List(Tag("topic", topic.id), Tag("group-id", topic.groupId))

    metricId match {

      case IN =>
        val metric = AtomicMetric[Long](metricPrefix, "in_messages", tags)
        val provider = new KafkaInMessagesProvider(kafkaConnector, topic)
        MetricProvider(metric, provider)

      case OUT =>
        val metric = AtomicMetric[Long](metricPrefix, "out_messages", tags)
        val provider = new KafkaOutMessagesProvider(kafkaConnector, topic)
        MetricProvider(metric, provider)

      case LAG =>
        val metric = AtomicMetric[Long](metricPrefix, "lag", tags)
        val provider = new KafkaLagProvider(kafkaConnector, topic)
        MetricProvider(metric, provider)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported kafka metric:[$metricId]")
    }
  }
}
