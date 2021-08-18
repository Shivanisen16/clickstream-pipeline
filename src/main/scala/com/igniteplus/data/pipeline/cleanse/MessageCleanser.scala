package com.igniteplus.data.pipeline.cleanse

import com.igniteplus.data.pipeline.constants.ApplicationConstants.{FORMAT, WRITER_FILE}
import com.igniteplus.data.pipeline.service.FileWriterService
import com.igniteplus.data.pipeline.service.FileWriterService.writeData
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, lower, row_number, trim, unix_timestamp, when}
import org.apache.spark.sql.types.StringType

object MessageCleanser {

   /***************** change datatype of time stamp ****************************************/
  def changeDataFormat (df: DataFrame, colName: Seq[String], dtype: Seq[String]): DataFrame = {

    var dfChangeDataType : DataFrame = df
    for(i <- colName.indices) {
      if(dtype(i) == "timestamp")
        dfChangeDataType = dfChangeDataType.withColumn(colName(i), unix_timestamp( col( colName(i)),"MM/dd/yyyy H:mm")
        .cast("timestamp"))
      else
        dfChangeDataType = dfChangeDataType.withColumn( colName(i), col( colName(i)).cast(dtype(i)) )
    }
    dfChangeDataType

  }

  /**************************** trim Column *******************************************************/
  def trimColumn (dfTemp: DataFrame) : DataFrame = {

    var dfTrimColumn : DataFrame = dfTemp
    var trimColumnList = dfTemp.schema.fields.filter(_.dataType.isInstanceOf[StringType])
    trimColumnList.foreach(f=>{
      dfTrimColumn = dfTrimColumn.withColumn(f.name,trim(col(f.name)))
    })
    dfTrimColumn

  }


  /*def checkForNullRow(df:DataFrame, columnList: Seq[String],filePath:String,fileFormat:String)(implicit spark:SparkSession): DataFrame = {

    val colName: Seq[Column] = columnList.map(ex => col(ex))
    val condition:Column = colName.map(ex => ex.isNull).reduce(_||_)
    val dfNotNullRows:DataFrame = df.withColumn("nullFlag" , when(condition,value = "true").otherwise(value = "false"))
    dfNotNullRows.show()

    // filter out all Null row in a dataframe
    val  dfNullRows:DataFrame = dfNotNullRows.filter(dfNotNullRows("nullFlag")==="true")

    //Write Null rows to a separate file
    if (dfNullRows.count() > 0)
      FileWriterService.writeData(dfNullRows, WRITER_FILE, FORMAT)

    FileWriterService.writeData(dfNotNullRows,"data/output/notNullData.csv","csv")
    dfNotNullRows
  }*/

  /*************************** filter rows with null values & write it to seperate file *******************/
  def checkForNullRow(df: DataFrame, primaryKeyColumns :Seq[String], filePath:String,fileFormat:String)(implicit spark:SparkSession) : DataFrame = {
    val primaryKeysAsColumnDataType : Seq[Column] = primaryKeyColumns.map(x => col(x))
    val condition : Column = primaryKeysAsColumnDataType.map(x => x.isNull).reduce(_||_)
    val nullFlag : DataFrame = df.withColumn("nullFlag",when(condition,"true").otherwise("false"))

    val notNullDF : DataFrame = nullFlag.filter("nullFlag==false")
    val nullDF : DataFrame = nullFlag.filter("nullFlag==true")
    val notNullDf : DataFrame = notNullDF.drop("nullFlag")


    writeData(nullDF, WRITER_FILE, FORMAT)

    writeData(notNullDf,"data/output/notNullData/notNullData.csv","csv")
    notNullDf
  }

  //ALternative: filter rows with null values & write it to seperate file
  /* def filterNullRows (dfTemp: DataFrame, colName: Seq[String])(implicit spark:SparkSession): DataFrame = {
    var dfNullRows = dfTemp
    for(i <- colName){
      dfNullRows = dfNullRows.filter(col(i).isNull)
    }
    if(dfNullRows.count() != 0) {
      FileWriterService.writeData(dfNullRows,WRITER_FILE,FORMAT)
    }
    dfNullRows
  }

  // drop rows with null values
  def dropNullRows(dfTemp: DataFrame, colName: Seq[String]) : DataFrame = {
    var dfFilterNotNullRows = dfTemp
    dfFilterNotNullRows = dfTemp.na.drop(colName)
    dfFilterNotNullRows
  }*/

  /************************************ drop duplicates ************************************************/
  def dropDuplicates(df: DataFrame,colName: Seq[String]): DataFrame={
    val dfDropDuplicates:DataFrame=df.dropDuplicates(colName)
    dfDropDuplicates

  }

  /************************************ de-duplication ************************************************/
  def removeDuplicateRows(dfTemp: DataFrame, orderByColumn: String) : DataFrame = {
    val windowSpec  = Window.partitionBy("session_id","item_id").orderBy(desc(orderByColumn))
    val dfDeduplicated: DataFrame = dfTemp.withColumn("row_number",row_number.over(windowSpec))
                                    .filter("row_number==1")
                                    .drop("row_number")
    dfDeduplicated
  }

  /*********************************** change to lowercase **********************************************/
  def changeToLowerCase(df: DataFrame, colName: Seq[String]) : DataFrame = {
    var dfLowerCase: DataFrame =df
    for(i <- colName.indices) {
      dfLowerCase = dfLowerCase.withColumn(colName(i), lower(col(colName(i))))
    }
    dfLowerCase
  }



}
