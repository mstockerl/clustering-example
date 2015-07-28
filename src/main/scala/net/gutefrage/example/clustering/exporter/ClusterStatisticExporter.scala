package net.gutefrage.example.clustering.exporter

import net.gutefrage.example.clustering.Constants
import net.gutefrage.example.clustering.model.{User, Cluster, ClusterInfo}
import net.gutefrage.example.index.model.{ElasticsearchClusterType, ElasticsearchReviewType}
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.{QueryBuilders, QueryBuilder}
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.terms.Terms
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.max.Max
import scala.collection.JavaConversions._

class ClusterStatisticExporter(client: Client, indexName: String) {

  def print(): Unit = {
    // Print "Noise"points
    printNoiseCluster

    Range(1, getMaxClusterId, 10000)
      .foreach ( id => printClusterInfo(id) )
  }

  private def printNoiseCluster: ClusterInfo = {
    val query = QueryBuilders.boolQuery()
      .mustNot(QueryBuilders
      .hasChildQuery("clusters", QueryBuilders.matchAllQuery())
      )
    fetchInfo("DocumentsWithoutClusters", query)
  }

  private def printClusterInfo(clusterId: Int): ClusterInfo = {
    val query = QueryBuilders.hasChildQuery(ElasticsearchClusterType.name, QueryBuilders.matchQuery("clusterId", clusterId))
    fetchInfo(clusterId.toString, query)
  }

  private def getMaxClusterId: Int = {
    val response = client.prepareSearch(indexName)
      .setTypes("clusters")
      .setSize(0)
      .addAggregation(
        AggregationBuilders
          .max("max_cluster_id")
          .field("clusterId")
      )
    .get()

    response.getAggregations.get[Max]("max_cluster_id").getValue.toInt
  }

  private def fetchInfo(clusterId: String, queryBuilder: QueryBuilder): ClusterInfo = {
    val response = client.prepareSearch(indexName)
      .setTypes(ElasticsearchReviewType.name)
      .setSize(5)
      .setNoFields()
      .setQuery(queryBuilder)
      .addAggregation(
        AggregationBuilders
          .terms("top_users")
          .field("user_id")
          .minDocCount(2)
      )
      .addAggregation(
        AggregationBuilders
          .avg("avg_ratings")
          .field("stars")
      )
      .get()


    val totalHits = response.getHits.getTotalHits
    val topUsersAggregation = response.getAggregations.get[Terms]("top_users")
    val avgAggregation = response.getAggregations.get[Avg]("sum_ratings")

    ClusterInfo(
      clusterId,
      totalHits.toInt,
      topUsersAggregation.getBuckets.map(user => User(user.getKey, user.getDocCount.toInt)).toList,
      avgAggregation.getValue)
  }
}
