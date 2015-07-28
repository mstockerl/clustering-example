package net.gutefrage.example.clustering

import net.gutefrage.example.clustering.algorithm.{SimilarityMatrixBuilder, DbScan}
import net.gutefrage.example.clustering.exporter.ClusterStatisticExporter
import net.gutefrage.example.clustering.writer.ResultWriter
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object Clustering {

  def main(args: Array[String]) {
    val clusterName: String = args(0)

    val settings = ImmutableSettings.settingsBuilder()
      .put("cluster.name", clusterName)
      .build()

    val client = new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))

    val similarityMatrixBuilder = new SimilarityMatrixBuilder(client, Constants.indexName)
    val similarityMatrix = similarityMatrixBuilder.build()

    new DbScan(similarityMatrix).run(Constants.clusterMinimalSimiliarity, Constants.clusterMinimalPoints)

    val resultWriter = new ResultWriter(client)
    resultWriter.writeCluster(similarityMatrix.getDocuments)

    val exporter = new ClusterStatisticExporter(client, Constants.indexName)
    exporter.print()
  }
}


