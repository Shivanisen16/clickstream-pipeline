package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.exception.FileWriterException
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeData (df: DataFrame,fileAdd: String, fileFormat: String)(implicit spark:SparkSession): Unit ={

    try {
      df.write.format(fileFormat)
        .option("header", "true")
        .mode("overwrite")
        .option("sep", ",")
        .save(fileAdd)
    }
    catch{
      case e: Exception => FileWriterException("Unable to write data to the location "+ s"$fileAdd")
    }

  }


}
