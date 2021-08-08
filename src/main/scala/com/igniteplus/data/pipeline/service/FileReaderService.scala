package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.exception.FileReaderException
import org.apache.spark.sql.{DataFrame, SparkSession}


object FileReaderService {

  def readData (fileAdd:String, fileformat:String)(implicit spark:SparkSession): DataFrame ={

    val df: DataFrame = try {
        spark.read.format(s"$fileformat")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(s"$fileAdd")
    }

    catch {
      //case e: FileNotFoundException => println("Couldn't find that file."): Unit
      //case e: IOException => println("Had an IOException trying to read that file")
      case e: Exception => FileReaderException("Unable to read file from "+ s"$fileAdd")
        spark.emptyDataFrame

    }
    val dfFileCount: Long = df.count()
    if(dfFileCount == 0)
      throw FileReaderException("Unable to read file from "+ s"$fileAdd")
    df

  }

}
