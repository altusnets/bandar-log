/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.infra.util

import org.slf4j.LoggerFactory

trait LogTrait { self =>

  @transient
  protected lazy val logger = LoggerFactory.getLogger(self.getClass)

}
