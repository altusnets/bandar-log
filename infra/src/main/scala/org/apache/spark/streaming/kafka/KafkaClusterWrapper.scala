package org.apache.spark.streaming.kafka

import com.aol.one.dwh.infra.config.KafkaConfig
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

class ErrorWrapper extends Err

class LeaderOffsetsWrapper(override val host: String, override val port: Int, override val offset: Long) extends LeaderOffset(host, port, offset)

class KafkaClusterWrapper(kafkaParams: Map[String, String]) extends KafkaCluster(kafkaParams)

object KafkaClusterWrapper {
  def apply(config: KafkaConfig): KafkaClusterWrapper = {
    val brokers = {
      lazy val brokersFromZK = ZkUtils.getAllBrokersInCluster(
        new ZkClient(config.zookeeperQuorum, Integer.MAX_VALUE, Integer.MAX_VALUE, ZKStringSerializer))
        .map(_.connectionString)
        .mkString(",")

      config.brokers.getOrElse(brokersFromZK)
    }
    val kafkaParams = Map("metadata.broker.list" -> brokers)
    new KafkaClusterWrapper(kafkaParams)
  }
}
