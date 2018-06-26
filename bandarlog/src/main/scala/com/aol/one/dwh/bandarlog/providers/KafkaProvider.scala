/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.connectors.KafkaConnector
import com.aol.one.dwh.bandarlog.metrics.AtomicValue
import com.aol.one.dwh.infra.config.Topic
import kafka.common.TopicAndPartition

/**
  * Kafka In Messages Provider
  *
  * Provides a count of input messages/heads over all topic partitions
  */
class KafkaInMessagesProvider(kafkaConnector: KafkaConnector, topic: Topic) extends Provider[Long] {

  override def provide(): AtomicValue[Long] = {
    AtomicValue(kafkaConnector.getHeads(topic).map(heads => heads.values.sum))
  }
}

/**
  * Kafka Out Messages Provider
  *
  * Provides a count of output messages/offsets over all topic partitions
  */
class KafkaOutMessagesProvider(kafkaConnector: KafkaConnector, topic: Topic) extends Provider[Long] {

  override def provide(): AtomicValue[Long] = {
    AtomicValue(kafkaConnector.getOffsets(topic).map(offsets => offsets.values.sum))
  }
}

/**
  * Kafka Lag Provider
  *
  * Provides lag value between in messages/heads and out messages/offsets per topic
  */
class KafkaLagProvider(kafkaConnector: KafkaConnector, topic: Topic) extends Provider[Long] {

  override def provide(): AtomicValue[Long] = {
    AtomicValue(kafkaConnector.getKafkaState(topic).map { case (heads, offsets) => getLag(heads, offsets)})
  }

  /**
    * Calculates lag Sum(HEAD - consumer) per topic
    *
    * @param heads     - leaders offsets, HEAD of kafka log
    * @param offsets   - consumers offsets
    * @return          - lag value
    */
  private def getLag(heads: Map[TopicAndPartition, Long], offsets: Map[TopicAndPartition, Long]): Long = {
    if (offsets.keySet.isEmpty) {
      heads.values.sum
    } else {
      val lags = heads.map { case (key, _) =>
        val lagValue = heads(key) - offsets.getOrElse(key, 0L)
        lagValue.max(0)
      }
      lags.sum
    }
  }
}
