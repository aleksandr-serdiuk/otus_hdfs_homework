import java.io.BufferedInputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import scala.collection.mutable.ListBuffer
import java.util.regex.Pattern

import org.apache.hadoop.io.IOUtils

object HDFSFileService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("core-site.xml")
  private val hdfsHDFSSitePath = new Path("hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)

  private val fileSystem = FileSystem.get(conf)

  def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  def getFolders(folderPath: String) = {
    val path = new Path(folderPath)
    val listFolders = new ListBuffer[Path]

    fileSystem.listStatus(path).foreach(folder => {
      if (folder.isDirectory)
        listFolders += folder.getPath
    })

    listFolders
  }

  def getFiles(folderPath: Path, with_inprogress: Boolean) = {
    val listFiles = new ListBuffer[Path]

    val regex_csv = "([^\\s]+(\\.(?i)(csv))$)";
    val pattern_csv = Pattern.compile(regex_csv)

    val regex_inprogress = "([^\\s]+(\\.(?i)(csv.inprogress))$)";
    val pattern_inprogress = Pattern.compile(regex_inprogress)

    fileSystem.listStatus(folderPath).foreach(entity => {
      if (entity.isFile) {
        val fileName = entity.getPath.getName

        if (pattern_csv.matcher(fileName).matches() | (pattern_inprogress.matcher(fileName).matches() & with_inprogress))
          listFiles += entity.getPath
      }
    })

    listFiles
  }

  def writeFile(srcPath: Path, dstPath: Path) = {
    if (fileSystem.exists(dstPath)) {
      val out = fileSystem.append(dstPath)
      out.writeBytes("\n")
      IOUtils.copyBytes(fileSystem.open(srcPath), out, conf)
    }
    else {
      val out = fileSystem.create(dstPath)
      IOUtils.copyBytes(fileSystem.open(srcPath), out, conf)
    }

  }

/*
  def writeFile(srcPath: Path, outPath: Path): Unit = {
    val outputStream = if (fileSystem.exists(outPath)) fileSystem.append(outPath) else fileSystem.create(outPath)

    val bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream))

    val stream = fileSystem.open(srcPath)

    def readLines = Stream.cons(stream.readLine, Stream.continually(stream.readLine))

    bufferedWriter.write(readLines.toString())
    bufferedWriter.write("\n")
    bufferedWriter.close()
  }
*/

  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }

  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

}
