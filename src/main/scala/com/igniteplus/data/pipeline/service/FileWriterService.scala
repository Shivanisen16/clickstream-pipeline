package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriterException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeData (dfTemp: DataFrame,fileAdd: String, fileformat: String)(implicit spark:SparkSession): Unit ={
    try{
      var dfWrite = dfTemp
      dfWrite.write.format("fileformat")
        .save("fileAdd")
    }
    catch{
      case e: Exception => FileWriterException("Unable to write to this location")
    }

  }


}
