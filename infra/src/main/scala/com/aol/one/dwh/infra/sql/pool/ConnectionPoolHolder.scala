/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql.pool

import java.util.concurrent.ConcurrentHashMap

import com.aol.one.dwh.infra.config.ConnectorConfig
import com.typesafe.config.Config

class ConnectionPoolHolder(mainConf: Config) {

  private val connectionPools = new ConcurrentHashMap[String, HikariConnectionPool]()

  def get(connectorConf: ConnectorConfig): HikariConnectionPool = {
    val poolKey = s"${connectorConf.connectorType}_${connectorConf.configId}"

    if (connectionPools.containsKey(poolKey)) {
      connectionPools.get(poolKey)
    } else {
      val connectionPool = new HikariConnectionPool(PoolConfig(connectorConf, mainConf))
      connectionPools.put(poolKey, connectionPool)
      connectionPool
    }
  }
}
