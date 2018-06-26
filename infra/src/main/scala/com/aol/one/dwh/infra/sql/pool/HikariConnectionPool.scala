/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.sql.pool

import java.sql.Connection

import com.aol.one.dwh.infra.util.LogTrait
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

class HikariConnectionPool(hikariConfig: HikariConfig) extends LogTrait {

  private lazy val dataSource = {
    logger.info(s"initializing connection pool:[${hikariConfig.getPoolName}] pool size: [${hikariConfig.getMaximumPoolSize}]")
    new HikariDataSource(hikariConfig)
  }

  def getName: String = hikariConfig.getPoolName

  def getConnection: Connection = dataSource.getConnection
}
