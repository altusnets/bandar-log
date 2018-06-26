/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.infra.config.Topic
import com.aol.one.dwh.bandarlog.connectors.KafkaConnector
import kafka.common.TopicAndPartition
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class KafkaLagProviderTest extends FunSuite with MockitoSugar {

  private val kafkaConnector = mock[KafkaConnector]
  private val topic = Topic("topic_id", Set("topic_1", "topic_2", "topic_3"), "group_id")

  test("check lag per topic") {
    val heads = Map(
      TopicAndPartition("topic_1", 1) -> 4L,
      TopicAndPartition("topic_2", 2) -> 5L,
      TopicAndPartition("topic_3", 3) -> 6L
    )

    val offsets = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )
    val kafkaState = Option((heads, offsets))
    when(kafkaConnector.getKafkaState(topic)).thenReturn(kafkaState)

    val result = new KafkaLagProvider(kafkaConnector, topic).provide()

    // topic       partition  heads  offsets  lag
    // topic_1     1          4      1        4-1=3
    // topic_2     2          5      2        5-2=3
    // topic_3     3          6      3        6-3=3
    assert(result.getValue.nonEmpty)
    assert(result.getValue.get == 9) // lag sum 3 + 3 + 3
  }

  test("check 0 lag case per topic") {
    val heads = Map(
      TopicAndPartition("topic_1", 1) -> 1L,
      TopicAndPartition("topic_2", 2) -> 2L,
      TopicAndPartition("topic_3", 3) -> 3L
    )

    val offsets = Map(
      TopicAndPartition("topic_1", 1) -> 4L,
      TopicAndPartition("topic_2", 2) -> 5L,
      TopicAndPartition("topic_3", 3) -> 6L
    )
    val kafkaState = Option((heads, offsets))
    when(kafkaConnector.getKafkaState(topic)).thenReturn(kafkaState)

    val result = new KafkaLagProvider(kafkaConnector, topic).provide()

    // topic       partition  heads  offsets  lag
    // topic_1     1          1      4        1-4= -3
    // topic_2     2          2      5        2-5= -3
    // topic_3     3          3      6        3-6= -3
    assert(result.getValue.nonEmpty)
    assert(result.getValue.get == 0) // lag.max(0) = 0
  }

  test("check lag for empty heads and offsets") {
    val kafkaState = Option((Map[TopicAndPartition, Long](), Map[TopicAndPartition, Long]()))
    when(kafkaConnector.getKafkaState(topic)).thenReturn(kafkaState)

    val result = new KafkaLagProvider(kafkaConnector, topic).provide()

    assert(result.getValue.nonEmpty)
    assert(result.getValue.get == 0)
  }

  test("return none if can't retrieve kafka state") {
    when(kafkaConnector.getKafkaState(topic)).thenReturn(None)

    val result = new KafkaLagProvider(kafkaConnector, topic).provide()

    assert(result.getValue.isEmpty)
  }
}
