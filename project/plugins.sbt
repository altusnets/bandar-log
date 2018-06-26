/*
  ******************************************************************************
  * Copyright 2018, Oath Inc.
  * Licensed under the terms of the Apache Version 2.0 license.
  * See LICENSE file in project root directory for terms.
  ******************************************************************************
*/

logLevel := Level.Warn

addSbtPlugin("com.typesafe.sbt"  % "sbt-native-packager"    % "1.2.0")
addSbtPlugin("com.typesafe.play" % "sbt-plugin"             % "2.4.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-coffeescript"       % "1.0.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-uglify"             % "1.0.3")
addSbtPlugin("com.typesafe.sbt"  % "sbt-digest"             % "1.1.0")
addSbtPlugin("com.typesafe.sbt"  % "sbt-gzip"               % "1.0.0")
addSbtPlugin("net.virtual-void"  % "sbt-dependency-graph"   % "0.8.1")
addSbtPlugin("com.eed3si9n"      % "sbt-assembly"           % "0.14.3")
addSbtPlugin("com.typesafe.sbt"  % "sbt-git"                % "0.8.5")
addSbtPlugin("org.scalastyle"    %% "scalastyle-sbt-plugin" % "0.8.0")