package com.microsoft.adf.kusto

object Log extends java.io.Serializable {
  @transient private lazy val logger = org.apache.log4j.Logger.getLogger(this.getClass())

  def info(msg:String) ={
    logger.info(msg)
  }
}