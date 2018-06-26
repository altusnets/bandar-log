/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog

import com.aol.one.dwh.infra.config.AppConfig
import com.aol.one.dwh.infra.util.LogTrait

object EntryPoint extends App with LogTrait {

  val config = AppConfig(args)
  val bandarlogs = new BandarlogsFactory(config).create()

  bandarlogs.foreach(_.execute())

  sys.addShutdownHook {
    bandarlogs.foreach(_.shutdown())
  }
}
