/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.connectors

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.KafkaClusterWrapper
import KafkaConnector._
import com.aol.one.dwh.infra.config.Topic
import com.aol.one.dwh.infra.util.LogTrait

object KafkaConnector {
  type KafkaPartitions = Set[TopicAndPartition]
  type KafkaHeads = Map[TopicAndPartition, Long]
  type KafkaOffsets = Map[TopicAndPartition, Long]
  type KafkaState = (KafkaHeads, KafkaOffsets)

  // Kafka API version, should be greater than 0 to read from non-ZK offset storage
  private val KAFKA_API_VERSION = 1.toShort
}

/**
  * Kafka Connector
  *
  * Provides Kafka cluster API by kafka config
  */
class KafkaConnector(kafkaCluster: KafkaClusterWrapper) extends LogTrait {

  def getKafkaState(topic: Topic): Option[KafkaState] =
    for {
      partitions <- getPartitions(topic)
      heads      <- getHeads(topic, partitions)
      offsets    <- getOffsets(topic, partitions)
    } yield (heads, offsets)

  def getHeads(topic: Topic): Option[KafkaHeads] = {
    getPartitions(topic).flatMap(partitions => getHeads(topic, partitions))
  }

  def getOffsets(topic: Topic): Option[KafkaOffsets] = {
    getPartitions(topic).flatMap(partitions => getOffsets(topic, partitions))
  }

  private def getPartitions(topic: Topic): Option[KafkaPartitions] = {
    kafkaCluster.getPartitions(topic.values) match {
      case Left(l) =>
        logger.error(s"Cannot obtain partitions for topic:[${topic.values}], cause {}", l.headOption.getOrElse(l.toString()))
        None
      case Right(r) =>
        logger.debug("Number of overall partitions for topic {} == {}", topic.values, r.size)
        Some(r)
    }
  }

  private def getHeads(topic: Topic, partitions: Set[TopicAndPartition]): Option[KafkaHeads] = {
    kafkaCluster.getLatestLeaderOffsets(partitions) match {
      case Left(l) =>
        logger.error(s"Cannot obtain leaders offsets for topic:[${topic.values}], cause {}", l.headOption.getOrElse(l.toString()))
        None
      case Right(r) => Some(r.map { case (key, value) => key -> value.offset })
    }
  }

  private def getOffsets(topic: Topic, partitions: Set[TopicAndPartition]): Option[KafkaOffsets] = {
    kafkaCluster.getConsumerOffsets(topic.groupId, partitions, KAFKA_API_VERSION) match {
      case Left(l) =>
        logger.error(s"Cannot obtain consumers offsets for topic:[${topic.values}], cause {}", l.headOption.getOrElse(l.toString()))
        None
      case Right(r) => Some(r)
    }
  }

}
