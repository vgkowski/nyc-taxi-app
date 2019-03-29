name := "emr-lab"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

// additional librairies
libraryDependencies ++= {
  Seq(
    "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
    "org.apache.hadoop" % "hadoop-aws" % "2.7.1",
    "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  )
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.commons.beanutils.**" -> "shadedstuff.beanutils.@1").inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0"),
  ShadeRule.rename("org.apache.commons.collections.**" -> "shadedstuff.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0"),
  ShadeRule.rename("org.apache.commons.collections.**" -> "shadedstuff2.collections.@1").inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
)