@Library('aol-on-jenkins-lib') _
def buildTimeout = 30
def sbtVersion = 'sbt-0.13.8'
DPflowSbtCompile (buildTimeout, sbtVersion,['clean scalastyle test compile bandarlog/docker:publishLocal'],['scalastyle test bandarlog/docker:publish'])