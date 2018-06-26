/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.connectors.KafkaConnector
import com.aol.one.dwh.bandarlog.metrics.BaseMetrics._
import com.aol.one.dwh.bandarlog.metrics.KafkaMetricFactoryTest._
import com.aol.one.dwh.bandarlog.providers.{KafkaInMessagesProvider, KafkaLagProvider, KafkaOutMessagesProvider}
import com.aol.one.dwh.infra.config.{Tag, Topic}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


object KafkaMetricFactoryTest {
  private val metricPrefix = "kafka_prefix"
  private val topic = Topic("some_topic_id", Set("topic-1", "topic-2"), "some_group_id")
  private val expectedTags = List(Tag("topic", topic.id), Tag("group-id", topic.groupId))
}

class KafkaMetricFactoryTest extends FunSuite with MockitoSugar {

  private val kafkaConnector = mock[KafkaConnector]
  private val kafkaMetricFactory = new KafkaMetricFactory(kafkaConnector)

  test("create kafka Metric & Provider for IN metric id") {
    val result = kafkaMetricFactory.create(IN, metricPrefix, topic)

    assertMetric(result.metric, "in_messages")
    assert(result.provider.isInstanceOf[KafkaInMessagesProvider])
  }

  test("create kafka Metric & Provider for OUT metric id") {
    val result = kafkaMetricFactory.create(OUT, metricPrefix, topic)

    assertMetric(result.metric, "out_messages")
    assert(result.provider.isInstanceOf[KafkaOutMessagesProvider])
  }

  test("create kafka Metric & Provider for LAG metric id") {
    val result = kafkaMetricFactory.create(LAG, metricPrefix, topic)

    assertMetric(result.metric, "lag")
    assert(result.provider.isInstanceOf[KafkaLagProvider])
  }

  test("throw exception in unknown metric case") {
    intercept[IllegalArgumentException] {
      kafkaMetricFactory.create("UNKNOWN_METRIC", metricPrefix, topic)
    }
  }

  private def assertMetric[V](metric: Metric[V], expectedName: String) = {
    assert(metric.prefix == metricPrefix)
    assert(metric.name == expectedName)
    assert(metric.tags == expectedTags)
    assert(metric.value == AtomicValue(None))
  }
}
