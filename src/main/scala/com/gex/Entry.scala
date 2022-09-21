package com.gex

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.events.S3Event
import com.gex.EcsTaskHandler.handleS3PutEvent
import org.apache.commons.logging.LogFactory

object Entry extends App {
    val logger = LogFactory.getLog(Entry.getClass.getName)

    logger.info("starting app")

    def handler(event: S3Event, context: Context): Unit = {
        event.getRecords.forEach(record => println(s"received record ${record.getS3.getObject.getKey}"))
        val fileName = event.getRecords.get(0).getS3.getObject.getKey
        handleS3PutEvent(fileName)
    }
}

