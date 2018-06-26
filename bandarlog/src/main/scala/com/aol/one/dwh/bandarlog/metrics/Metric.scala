/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.infra.config.Tag

/**
  * Metric
  */
trait Metric[V] {

  /**
    * Metric prefix name
    */
  def prefix: String

  /**
    * Metric name
    */
  def name: String

  /**
    * Metric reporter tags
    */
  def tags: List[Tag]

  /**
    * Parametrized metric value
    */
  def value: Value[V]
}

/**
  * Parametrized Metric Value
  */
trait Value[V] {

  def setValue(v: Option[V]): Unit

  def getValue: Option[V]
}
