package com.igniteplus.data.pipeline
import com.igniteplus.data.pipeline.constants.ApplicationConstants
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{APP_NAME, MASTER}
import com.igniteplus.data.pipeline.exception.FileReaderException
import com.igniteplus.data.pipeline.service.PipelineService
import org.apache.spark.sql.{ SparkSession}
import com.igniteplus.data.pipeline.util.ApplicationUtil

object clickStreamAnalysis {
  def main(args: Array[String]): Unit = {

      implicit val spark: SparkSession = ApplicationUtil.createSparkSession(APP_NAME, MASTER)
      try{
        PipelineService.executePipeline()
      }

      catch {
        case ex : FileReaderException =>
          println(ex)
          sys.exit(ApplicationConstants.FAILURE_EXIT_CODE)
      }

  }
}
