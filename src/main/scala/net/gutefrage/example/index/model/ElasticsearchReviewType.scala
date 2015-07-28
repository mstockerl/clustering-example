package net.gutefrage.example.index.model

import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

object ElasticsearchReviewType {

    def name: String = "review"

    def mapping: XContentBuilder = {
        XContentFactory.jsonBuilder().startObject()
            .startObject(name)
                .startObject("properties")
                    .startObject("business_id")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("user_id")
                        .field("type", "string")
                        .field("index", "not_analyzed")
                    .endObject()
                    .startObject("stars")
                        .field("type", "double")
                    .endObject()
                    .startObject("text")
                        .field("type", "string")
                        .field("term_vector", "with_positions_offsets_payloads")
                    .endObject()
                    .startObject("date")
                        .field("type", "date")
                    .endObject()
                .endObject()
            .endObject()
        .endObject()
    }

    def toEsContent(model: Review): XContentBuilder = {
        XContentFactory.jsonBuilder()
          .startObject()
            .field("business_id", model.business_id)
            .field("user_id", model.user_id)
            .field("stars", model.stars)
            .field("date", model.date)
            .field("text", model.text)
        .endObject()
    }
}
