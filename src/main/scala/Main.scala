import HDFSFileService._
import org.apache.hadoop.fs._

object Main {
  def main(args: Array[String]): Unit = {

    // docker exec namenode hdfs dfs -put /sample_data/stage /user/Aleksandr.Serdiuk
    // docker exec namenode hdfs dfs -get /user/Aleksandr.Serdiuk/ods /sample_data/ods

    val src_folder = "stage"
    val dst_folder = "ods"

    // create dst folder if not exists
    createFolder(dst_folder)

    // get folders in source directory
    val listFolders = getFolders(src_folder)

    listFolders.foreach(folder => {
      println(s"Folder: ${folder.getName}")

      // get .csv files (if with_inprogress .csv.inprogress as well)
      val listFiles = getFiles(folder, true)

      val dateFolder = dst_folder + "/" + folder.getName
      // create folder for date partition
      createFolder(dateFolder)

      if (listFiles.nonEmpty) {
        val resultFile = new Path(dateFolder + "/part-0000.csv")
        listFiles.foreach(file => {
          println(s"   File: ${file.getName}")
          // create merged file
          writeFile(file, resultFile)
        })
      }

    })

  }
}

