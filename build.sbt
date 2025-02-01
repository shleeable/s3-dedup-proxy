val scala3Version = "3.5.2"

enablePlugins(JavaAppPackaging)

lazy val root = project
  .in(file("."))
  .settings(
    name := "s3-dedup-proxy",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    Compile / mainClass := Some("com.jortage.poolmgr.Poolmgr"),
    unmanagedSources / excludeFilter := HiddenFileFilter
      || "EncryptedBlobStore.java"
      || "S3ProxyExtension.java"
      || "S3ProxyRule.java"
      || "Main.java",

    libraryDependencies += "blue.endless" % "jankson" % "1.2.3",
    libraryDependencies += "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.15.2",
    libraryDependencies += "com.fasterxml.woodstox" % "woodstox-core" % "6.5.1",
    libraryDependencies += "com.google.code.findbugs" % "findbugs-annotations" % "3.0.1",
    libraryDependencies += "com.google.code.findbugs" % "jsr305" % "3.0.2",
    libraryDependencies += "com.squareup.okhttp3" % "okhttp" % "4.11.0",
    libraryDependencies += "com.squareup.okhttp3" % "okhttp-brotli" % "4.11.0",
    libraryDependencies += "com.zaxxer" % "HikariCP" % "5.0.1",
    libraryDependencies += "commons-fileupload" % "commons-fileupload" % "1.5",
    libraryDependencies += "org.apache.jclouds" % "jclouds-blobstore" % "2.5.0",
    libraryDependencies += "org.apache.jclouds.api" % "filesystem" % "2.5.0",
    libraryDependencies += "org.apache.jclouds.driver" % "jclouds-slf4j" % "2.5.0",
    libraryDependencies += "org.apache.jclouds.provider" % "aws-s3" % "2.5.0",
    libraryDependencies += "org.apache.jclouds.provider" % "b2" % "2.5.0",
    libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "11.0.16",
    libraryDependencies += "org.mariadb.jdbc" % "mariadb-java-client" % "3.2.0",
    libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.36",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.36",

    libraryDependencies += "org.tpolecat" %% "skunk-core" % "0.6.4",

    libraryDependencies += "org.scalameta" %% "munit" % "1.1.0" % Test,
    libraryDependencies += "org.typelevel" %% "munit-cats-effect" % "2.0.0" % Test,
  )
