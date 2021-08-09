package com.igniteplus.data.pipeline.constants

object ApplicationConstants {

    val FORMAT = "csv"

    val APP_NAME: String = "product"
    val MASTER: String = "local"

    //column name
    val EVENT_TIMESTAMP : String = "event_timestamp"
    val ITEM_PRICE : String = "item_price"
    val DEPARTMENT : String = "department_name"
    val PRODUCT_TYPE: String = "product_type"
    val REDIRECTION_SOURCE : String = "redirection_source"
    val ITEM_ID : String = "item_id"
    val SESSION_ID : String = "session_id"

    val FAILURE_EXIT_CODE : Int = 1

    //file path
    val ITEMDATA = "data/input/item/item_data.csv"
    val LOGDATA = "data/input/clickstream/clickstream_log.csv"
    val WRITER_FILE = "Writerfile/writeNullRows.csv"

    //Column Data name
    val COL_DATANAME_ITEMDATA : Seq[String] = Seq(ApplicationConstants.ITEM_PRICE)
    val COL_DATANAME_LOGDATA : Seq[String] = Seq(ApplicationConstants.EVENT_TIMESTAMP)

    //Column Data types
    val DATATYPE_ITEMDATA: Seq[String] = Seq("float")
    val DATATYPE_LOGDATA: Seq[String] = Seq("timestamp")

    //Column Name with null values
    val PRIMARY_KEY_ITEMDATA: Seq[String] = Seq(ApplicationConstants.ITEM_ID)
    val PRIMARY_KEY_LOGDATA: Seq[String] = Seq(ApplicationConstants.SESSION_ID, ApplicationConstants.ITEM_ID)

    //column Name to drop duplicates
    val COLUMN_DUPLICATE_ITEMDATA : Seq[String] = Seq(ApplicationConstants.ITEM_ID)

    //column to be orderBy
    val COLUMN_ORDERBY_LOGDATA: String = "event_timestamp"

    //Column to change to lower case
    val COLUMN_LOWERCASE_ITEMDATA : Seq[String] = Seq(ApplicationConstants.DEPARTMENT)
    val COLUMN_LOWERCASE_LOGDATA : Seq[String] = Seq(ApplicationConstants.REDIRECTION_SOURCE)

}
