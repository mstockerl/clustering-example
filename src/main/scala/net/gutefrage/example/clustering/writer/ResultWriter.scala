package net.gutefrage.example.clustering.writer

import net.gutefrage.example.clustering.Constants
import net.gutefrage.example.clustering.model.{ClusterType, Document}
import net.gutefrage.example.index.model.ElasticsearchClusterType
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}

class ResultWriter(client: Client) {

  def writeCluster(documents: List[Document]): Unit = {
    var bulk: BulkRequestBuilder = client.prepareBulk()
    var counter = 0

    documents.foreach {
      doc =>
        val builder = getJson(doc)

        bulk.add(client.prepareIndex(Constants.indexName, ElasticsearchClusterType.name)
          .setParent(doc.id.toString)
          .setSource(builder))

        if (counter == 10000) {
          bulk.get()
          bulk = client.prepareBulk()
          counter = 0
        }

        counter += 1
    }

    if (bulk.numberOfActions() > 0)
      bulk.get()
  }

  private def getJson(document: Document): XContentBuilder = {
    val jsonBuilder = XContentFactory.jsonBuilder().startObject()


    val documentSpecificJson = document.clusterType match {
      case Some(ClusterType.CLUSTER) =>
        jsonBuilder
        .field("clusterId", document.clusterId.getOrElse(throw new scala.Exception(s"No cluster id for $document")))
        .field("clusterType", "cluster")
      case Some(ClusterType.NOISE) =>
        jsonBuilder
        .field("clusterType", "noise")
      case Some(ClusterType.EDGE_POINT) =>
        jsonBuilder
          .field("clusterType", "edge_point")
      case _ => throw new Exception(s"$document has no cluster type!")
    }

    documentSpecificJson.endObject()
  }

}
