/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

package com.aol.one.dwh.bandarlog.scheduler

import java.util.{Timer, TimerTask}

import com.aol.one.dwh.infra.config.SchedulerConfig

/**
  * Scheduler
  *
  * Run function by schedule
  *
  * @param config - scheduler config with delay and schedule period
  */
class Scheduler(config: SchedulerConfig) {

  private val timer = new Timer()

  def schedule(fun: () => Unit): Unit = {
    timer.schedule(createTask(fun), config.delayPeriod.toMillis, config.schedulingPeriod.toMillis)
  }

  def shutdown(): Unit = {
    timer.cancel()
  }

  private def createTask(fun: () => Unit): TimerTask = new TimerTask {
    override def run(): Unit = fun()
  }

}
