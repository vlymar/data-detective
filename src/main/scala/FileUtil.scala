object FileUtil {
  import java.io.File

  def getListOfFiles(dir: String): List[File] =
    new File(dir) match {
      case f if f.exists && f.isDirectory =>
        f.listFiles.toList.map(_.toString).flatMap(getListOfFiles(_))
      case f if f.exists && f.isFile => List(f)
      case _ => List[File]()
    }

  def fileStream(files: List[File]): Stream[String] =
    files match {
      case f :: fs => readFile(f) #:: fileStream(fs)
      case Nil => Stream.empty
    }

  def readFile(file: File): String = {
    val source = io.Source.fromFile(file)
    try source.mkString finally source.close
  }

  def writeFile(name: String, body: String) = {
    val file = new File(name)
    val bw = new java.io.BufferedWriter(
      new java.io.FileWriter(file)
    )
    bw.write(body)
    bw.close()
  }
}

