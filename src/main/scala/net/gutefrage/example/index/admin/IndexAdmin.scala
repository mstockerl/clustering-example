package net.gutefrage.example.index.admin

import net.gutefrage.example.index.model.{ElasticsearchClusterType, ElasticsearchReviewType}
import org.elasticsearch.client.AdminClient
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.unit.TimeValue

/**
 * Handles the index creation
 *
 * @param adminClient, Admin Client of ES
 * @param timeoutInMillis, timeout for responses of ES
 */
class IndexAdmin(adminClient: AdminClient, timeoutInMillis: Long) {
    val timeoutValue = TimeValue.timeValueMillis(timeoutInMillis)

    /**
     * Creates the given index, if it does not exist already.
     * The index is initialized with the Mapping defined in [[ElasticsearchReviewType]]
     *
     * @param indexName name of the index to be created
     * @param primaryShards number of primary shards
     * @param replicaShards number of replica shards
     */
    def createIndexIfNotExists(indexName: String,
                               primaryShards: Int,
                               replicaShards: Int): Unit = {
        if (!indexExists(indexName)) {
            println("Create index!")
            createIndex(indexName, primaryShards, replicaShards)
            waitUntilCreated(indexName)
        }
    }

    /**
     * Refresh the given index
     *
     * @param indexName index to be refreshed
     */
    def refreshIndex(indexName: String): Unit = {
      adminClient
        .indices()
        .prepareRefresh(indexName)
        .get(timeoutValue)
    }

    private def createIndex(indexName: String, primaryShards: Int, replicaShards: Int): Unit = {
        val createIndexRequestBuilder = adminClient
          .indices()
          .prepareCreate(indexName)
          .setSettings(buildSettings(primaryShards, replicaShards))

        createIndexRequestBuilder.addMapping(ElasticsearchReviewType.name, ElasticsearchReviewType.mapping)
        createIndexRequestBuilder.addMapping(ElasticsearchClusterType.name, ElasticsearchClusterType.mapping)
        createIndexRequestBuilder.get(timeoutValue)
    }

    private def buildSettings(primaryShards: Int, replicaShards: Int): Settings.Builder = {
        ImmutableSettings.builder()
            .put("number_of_shards", primaryShards)
            .put("number_of_replicas", replicaShards)
    }

    private def indexExists(indexName: String): Boolean = {
        adminClient
          .indices()
          .prepareExists(indexName)
          .get(timeoutValue).isExists
    }

    private def waitUntilCreated(indexName: String): Unit = {
        adminClient
          .cluster()
          .prepareHealth(indexName)
          .setWaitForYellowStatus()
          .get(timeoutValue)
    }

}
