/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.providers

import com.aol.one.dwh.bandarlog.metrics.AtomicValue
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar


class SqlLagProviderTest extends FunSuite with MockitoSugar {

  private val fromProvider = mock[SqlTimestampProvider]
  private val toProvider = mock[SqlTimestampProvider]
  private val lagProvider = new SqlLagProvider(fromProvider, toProvider)

  test("check lag between from and to providers") {
    val fromValue = AtomicValue(Some(7L))
    val toValue = AtomicValue(Some(4L))

    when(fromProvider.provide()).thenReturn(fromValue)
    when(toProvider.provide()).thenReturn(toValue)

    val lag = lagProvider.provide()

    assert(lag.getValue.nonEmpty)
    assert(lag.getValue.get == 3)
  }

  test("return none if 'from provider' value is none") {
    val toValue = AtomicValue(Some(4L))

    when(fromProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toProvider.provide()).thenReturn(toValue)

    val lag = lagProvider.provide()

    assert(lag.getValue.isEmpty)
  }

  test("return none if 'to provider' value is none") {
    val fromValue = AtomicValue(Some(7L))

    when(fromProvider.provide()).thenReturn(fromValue)
    when(toProvider.provide()).thenReturn(AtomicValue[Long](None))

    val lag = lagProvider.provide()

    assert(lag.getValue.isEmpty)
  }

  test("return none if both providers values is none") {
    when(fromProvider.provide()).thenReturn(AtomicValue[Long](None))
    when(toProvider.provide()).thenReturn(AtomicValue[Long](None))

    val lag = lagProvider.provide()

    assert(lag.getValue.isEmpty)
  }
}
