package com.igniteplus.data.pipeline.service

import org.apache.spark.sql.{DataFrame, SparkSession}

object FileReaderService {

  def readData (fileAdd:String, fileformat:String)(implicit spark:SparkSession): DataFrame ={

    val df = spark.read.format(s"$fileformat")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(s"$fileAdd")

    df
  }

}
