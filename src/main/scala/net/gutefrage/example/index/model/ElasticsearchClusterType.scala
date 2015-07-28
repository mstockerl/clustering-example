package net.gutefrage.example.index.model

import org.elasticsearch.common.xcontent.{XContentFactory, XContentBuilder}

object ElasticsearchClusterType {

  def name: String = "clusters"

  def mapping: XContentBuilder = {
    XContentFactory.jsonBuilder().startObject()
      .startObject(name)
        .startObject("_parent")
          .field("type", ElasticsearchReviewType.name)
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
