/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.metrics

import com.aol.one.dwh.bandarlog.providers.Provider

/**
  * Class holder for metric -> provider pair
  */
case class MetricProvider[V](metric: Metric[V], provider: Provider[V])
