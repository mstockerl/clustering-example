package net.gutefrage.example.index.loader

import net.gutefrage.example.index.model.Review
import org.json4s.jackson.Serialization

import scala.collection.mutable.ListBuffer
import org.json4s.NoTypeHints

import scala.io.Source
class DataLoader(filePath: String, batchSize: Int) {

    val lines = Source.fromFile(filePath).getLines()

    implicit val formats = Serialization.formats(NoTypeHints)

    def loadReviewBatch(): Option[List[Review]] = {
        var counter = 0
        if (lines.hasNext) {
            val list = new ListBuffer[Review]()
            while (lines.hasNext && counter < batchSize) {
                val line = lines.next()
                list += Serialization.read[Review](line)
                counter += 1
            }

            Some(list.toList)
        } else {
            None
        }
    }
    
    def hasNext: Boolean = lines.hasNext
}