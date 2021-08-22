package com.igniteplus.data.pipeline.transform

import org.apache.spark.sql.DataFrame


object JoinService {

  def joinOperation(df1: DataFrame, df2: DataFrame, joinKey: String, jointype: String): DataFrame = {
    val joinedTable = df1.join(df2, df1(joinKey) === df2(joinKey),jointype)
    joinedTable
  }

  def joinTable(df1: DataFrame, df2: DataFrame, joinKey: String, jointype: String): DataFrame = {
    val joinedTable = df1.join(df2,Seq(joinKey),jointype)
    joinedTable
  }
}
