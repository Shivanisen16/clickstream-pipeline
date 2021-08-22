package com.igniteplus.data.pipeline.service

import com.igniteplus.data.pipeline.cleanse.MessageCleanser
import com.igniteplus.data.pipeline.cleanse.MessageCleanser.checkForNullRow
import com.igniteplus.data.pipeline.constants.ApplicationConstants.{COLUMN_DUPLICATE_ITEMDATA, COLUMN_LOWERCASE_ITEMDATA, COLUMN_LOWERCASE_LOGDATA, COLUMN_ORDERBY_LOGDATA, COL_DATANAME_ITEMDATA, COL_DATANAME_LOGDATA, DATATYPE_ITEMDATA, DATATYPE_LOGDATA, FORMAT, ITEMDATA, ITEM_ID, ITEM_PRICE, JOIN_TYPE_NAME, LOGDATA, PRIMARY_KEY_ITEMDATA, PRIMARY_KEY_LOGDATA, WRITER_FILE}
import com.igniteplus.data.pipeline.service.FileWriterService.writeData
import com.igniteplus.data.pipeline.transform.JoinService
import com.igniteplus.data.pipeline.transform.JoinService.joinTable
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.execution.command.ResetCommand.printSchema
import org.apache.spark.sql.functions.broadcast
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
    val itemDfNotNull = MessageCleanser.checkForNullRow(itemDf,PRIMARY_KEY_ITEMDATA,WRITER_FILE,FORMAT)
    val logDfNotNull = MessageCleanser.checkForNullRow(logDf,PRIMARY_KEY_LOGDATA,WRITER_FILE,FORMAT)
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
    println(itemDfLowerCase.count())
    println(logDfLowerCase.count())


    /************************* join tables ***************************************************************/
    /*//broadcast join
    val joinedDf = JoinService.joinTable(itemDfLowerCase,logDfLowerCase,ITEM_ID)
    joinedDf.show(false)
    println(joinedDf.count())

    //shuffle join
    spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 2)
    val dfJoined = itemDfLowerCase.join( logDfLowerCase, itemDfLowerCase("item_id") === logDfLowerCase("item_id") )
    dfJoined.show()
    println(dfJoined.count())


    val leftJoinDf = JoinService.joinTable(logDfLowerCase,itemDfLowerCase,ITEM_ID,"left")
    leftJoinedDf.show(false)
    println(leftJoinedDf.count()) */

    val leftJoinedDf = JoinService.joinTable(logDfLowerCase,itemDfLowerCase,ITEM_ID, JOIN_TYPE_NAME)
    leftJoinedDf.show(false)
    println(leftJoinedDf.count())

  }


}
