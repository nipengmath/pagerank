object Config {
  val home = System.getenv("HOME") + "/"
  val resourcesPath = getClass.getResource("/").getPath
}