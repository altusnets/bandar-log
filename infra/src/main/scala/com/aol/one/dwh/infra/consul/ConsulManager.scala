/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.consul

import com.aol.one.dwh.infra.util.LogTrait
import com.ecwid.consul.ConsulException
import com.ecwid.consul.v1.kv.model.GetValue
import com.ecwid.consul.v1.{ConsulClient, Response}
import org.apache.commons.codec.binary.Base64


/**
  * Consul Manager
  *
  * Provides values from Consul by path and key
  */
class ConsulManager(consulClient: ConsulClient) extends LogTrait {

  def getFlag(path: String): Option[String] =
    try {
      fromOptional(consulClient.getKVValue(path))
    } catch {
      case e: ConsulException =>
        logger.warn(s"Could not access Consul instance to get flag", e)
        None
    }

  private def fromOptional[T >: String](x: Response[GetValue]): Option[T] = {
    Option(x.getValue).flatMap(x => decodeBase64(x.getValue))
  }

  private def decodeBase64(value: String): Option[String] = {
    Option(Base64.decodeBase64(value)).map(new String(_))
  }
}
