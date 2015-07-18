resolvers += Resolver.url("bintray-danosipov-sbt-plugin-releases",
  url("http://dl.bintray.com/content/danosipov/sbt-plugins"))(Resolver.ivyStylePatterns)

addSbtPlugin("com.danosipov" % "sbt-scalding-plugin" % "1.0.3")
