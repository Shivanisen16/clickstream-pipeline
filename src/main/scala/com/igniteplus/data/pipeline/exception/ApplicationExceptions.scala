package com.igniteplus.data.pipeline.exception

class ApplicationException (message :String, cause:Throwable)extends Exception(message, cause)
{
  def this(message: String) = this(message,None.orNull)
}
case class FileReaderException(message: String) extends ApplicationException(message)
case class FileWriterException(message: String) extends ApplicationException(message)

