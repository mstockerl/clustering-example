package net.gutefrage.example.index.index

import net.gutefrage.example.index.model.{ElasticsearchReviewType, Review}
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.action.index.IndexRequestBuilder

class BulkInsert(client: Client, indexName: String, timeoutInMillis: Long) {
    val timeoutValue = TimeValue.timeValueMillis(timeoutInMillis)

    def insert(elements: List[Review]): Unit = {
        val bulkInsert = client.prepareBulk()

        elements.foreach(element => {
            val indexRequestBuilder = indexRequest(element)
            bulkInsert.add(indexRequestBuilder)
        })

        bulkInsert.get(timeoutValue)
    }

    private def indexRequest(review: Review): IndexRequestBuilder = {
        val reviewContent = ElasticsearchReviewType.toEsContent(review)

        client
          .prepareIndex(indexName, ElasticsearchReviewType.name, review.review_id)
          .setSource(reviewContent)
    }

}
