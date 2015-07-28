package net.gutefrage.example.index

import net.gutefrage.example.clustering.Constants
import net.gutefrage.example.index.admin.IndexAdmin
import net.gutefrage.example.index.index.BulkInsert
import net.gutefrage.example.index.loader.DataLoader
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.client.Client

import scala.annotation.tailrec

object Indexer {

    // Dataset configuration
    private val batchSize: Int = 100

    // Index configuration
    private val primaryShards = 1
    private val replicaShards = 0
    private val timeout: Long = 100000

    def main(args: Array[String]): Unit = {
      val clusterName: String = args(0)
      val dataPath: String = args(1)

      val settings = ImmutableSettings.builder().put("cluster.name", clusterName)

      val client: Client = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))

      val dataImporter = new DataLoader(dataPath, batchSize)
      val indexer = new BulkInsert(client, Constants.indexName, timeout)

      println("Start running test programm!")

      try {
        createIndex(client)
        indexDataset(indexer, dataImporter)
      } finally {
        client.close()
      }
    }
    
    private def createIndex(client: Client): IndexAdmin = {
        val indexAdmin = new IndexAdmin(client.admin(), timeout)
        indexAdmin.createIndexIfNotExists(Constants.indexName, primaryShards, replicaShards)

        indexAdmin
    }

    private def indexDataset(indexer: BulkInsert, dataImporter: DataLoader): Unit = {
        @tailrec
        def indexReviewBatch(): Unit = {
          val batchOpt = dataImporter.loadReviewBatch()

          batchOpt.foreach(batch => indexer.insert(batch))

          if (dataImporter.hasNext) {
            indexReviewBatch()
          }
        }

        println("Index reviews")
        indexReviewBatch()
    }
}
