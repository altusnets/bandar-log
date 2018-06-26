/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import java.util.concurrent.atomic.AtomicReference
import com.aol.one.dwh.infra.config.Tag

case class AtomicMetric[V](prefix: String, name: String, tags: List[Tag], value: Value[V] = AtomicValue[V](None)) extends Metric[V]

case class AtomicValue[V](initValue: Option[V]) extends Value[V] {

  private val atomicValue = new AtomicReference[Option[V]](initValue)

  override def setValue(value: Option[V]): Unit = atomicValue.set(value)

  override def getValue: Option[V] = atomicValue.get()
}
