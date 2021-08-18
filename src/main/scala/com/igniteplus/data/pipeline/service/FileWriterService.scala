package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriterException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeData (df: DataFrame,filePath: String, fileFormat: String)(implicit spark:SparkSession): Unit ={

    try {
      df.write
        .format(fileFormat)
        .option("path", filePath)
        .mode("overwrite")
        .save()
    }
    catch{
      case e: Exception => FileWriterException("Unable to write data to the location "+ s"$filePath")
    }

  }


}
