/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

import com.typesafe.sbt.GitVersioning
import com.typesafe.sbt.SbtGit.git
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport.Universal
import sbt.Keys._
import sbt._
import com.typesafe.sbt.packager.Keys.packageName
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.universal.UniversalDeployPlugin

object DockerSupportPlugin extends AutoPlugin {

  override def requires: sbt.Plugins = JavaAppPackaging && GitVersioning && UniversalDeployPlugin

  val defaultRepository = Some("main-virtual.docker.vidible.aolcloud.net") // TODO: change default repository
  val organizationName = "main"

  override lazy val projectSettings = Seq(
    version in Docker := s"${version.value}-${git.gitHeadCommit.value.get.substring(0, 8)}-${git.formattedDateVersion.value}",
    dockerRepository := defaultRepository,
    dockerUpdateLatest := true,
    packageName := s"$organizationName/${packageName.value}",
    mappings in Universal ++= {
    val dir = baseDirectory.value / "scripts"
    ((dir.*** --- dir) x relativeTo(dir)).map( el => el._1 -> ("bin/" + el._2))
  })

}
