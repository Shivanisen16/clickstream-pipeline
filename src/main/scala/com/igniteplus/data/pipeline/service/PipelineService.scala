package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanse.MessageCleanser
import com.igniteplus.data.pipeline.cleanse.MessageCleanser.dropDuplicates
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{COLUMN_DUPLICATE_ITEMDATA, COLUMN_LOWERCASE_ITEMDATA, COLUMN_LOWERCASE_LOGDATA, COLUMN_ORDERBY_LOGDATA, COL_DATANAME_ITEMDATA, COL_DATANAME_LOGDATA, DATATYPE_ITEMDATA, DATATYPE_LOGDATA, FORMAT, ITEMDATA, LOGDATA, NULL_COLUMN_NAME_ITEMDATA, NULL_COLUMN_NAME_LOGDATA}
import com.igniteplus.data.pipeline.util.ApplicationUtil.createSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

object PipelineService {

  def executePipeline() (implicit spark: SparkSession): Unit = {


    /************************** show both file data *************************************************/
    val itemDf: DataFrame  = FileReaderService.readData(ITEMDATA,FORMAT)
    val logDf: DataFrame = FileReaderService.readData(LOGDATA,FORMAT)
    itemDf.show(false)
    logDf.show(false)

    /************************** change datatype *****************************************************/
    val itemDfChangedDatatype = MessageCleanser.changeDataFormat(itemDf,COL_DATANAME_ITEMDATA, DATATYPE_ITEMDATA)
    val logDfChangedDatatype = MessageCleanser.changeDataFormat(logDf,COL_DATANAME_LOGDATA, DATATYPE_LOGDATA)
    itemDfChangedDatatype.show(false)
    logDfChangedDatatype.show(false)

    /************************** trim columns ********************************************************/
    val itemDfTrimmed= MessageCleanser.trimColumn(itemDfChangedDatatype)
    val logDfTrimmed = MessageCleanser.trimColumn(logDfChangedDatatype)
    itemDfTrimmed.show(false)
    logDfTrimmed.show(false)

    /************************** write rows with null values into seperate file **************************************/
    val itemDfNullRows = MessageCleanser.filterNullRows(itemDfTrimmed, NULL_COLUMN_NAME_ITEMDATA)
    val logDfNullRows = MessageCleanser.filterNullRows(logDfTrimmed, NULL_COLUMN_NAME_LOGDATA)
    itemDfNullRows.show(false)
    logDfNullRows.show(false)


    /************************** remove rows with null values ********************************************************/
    val itemDfNotNull = MessageCleanser.dropNullRows(itemDfTrimmed, NULL_COLUMN_NAME_ITEMDATA)
    val logDfNotNull = MessageCleanser.dropNullRows(logDfTrimmed, NULL_COLUMN_NAME_LOGDATA)
    itemDfNotNull.show(false)
    logDfNotNull.show(false)

    /**************************** deduplication *******************************************************************/
    val itemDfDedupliacted = MessageCleanser.dropDuplicates(itemDfNotNull,COLUMN_DUPLICATE_ITEMDATA)
    val logDfDeduplicated = MessageCleanser.removeDuplicateRows(logDfNotNull,COLUMN_ORDERBY_LOGDATA)
    itemDfDedupliacted.show(false)
    logDfDeduplicated.show(false)

    /*************************** change to lower case ***************************************************************/
    val itemDfLowerCase = MessageCleanser.changeToLowerCase(itemDfDedupliacted,COLUMN_LOWERCASE_ITEMDATA)
    val logDfLowerCase = MessageCleanser.changeToLowerCase(logDfDeduplicated,COLUMN_LOWERCASE_LOGDATA)
    itemDfLowerCase.show(false)
    logDfLowerCase.show(false)


  }


}
