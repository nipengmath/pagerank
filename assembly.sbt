assemblyOption in assembly := (assemblyOption in assembly).value.copy(
        includeScala = false, includeDependency = false)

mainClass in assembly := Some("PageRank2")
