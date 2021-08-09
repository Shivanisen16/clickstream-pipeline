package com.igniteplus.data.pipeline.service
import com.igniteplus.data.pipeline.exception.FileReaderException
import com.igniteplus.data.pipeline.service.FileWriterService.writeData
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
      case e: Exception => FileReaderException("Unable to read file from "+ s"$fileAdd")
        spark.emptyDataFrame
    }
    FileWriterService.writeData(df,"data/output/readData.csv","csv")

    val dfFileCount: Long = df.count()
    if(dfFileCount == 0)
      throw FileReaderException("Unable to read file from "+ s"$fileAdd")
    df
  }

}