package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileWriterService {

  def writeData (dfTemp: DataFrame,fileAdd: String, fileformat: String)(implicit spark:SparkSession): Unit ={
      var dfWrite = dfTemp
      dfWrite.write.format("fileformat")
      .save("fileAdd")

  }


}
