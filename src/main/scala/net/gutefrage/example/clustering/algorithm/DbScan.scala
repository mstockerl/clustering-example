package net.gutefrage.example.clustering.algorithm

import net.gutefrage.example.clustering.model.{ClusterType, Document}

import scala.collection.mutable.ListBuffer
import scala.util.Try

class DbScan(similarityMatrix: SimilarityMatrix) {

  def run(minSim: Double, minPoints: Int) {

    var currentCluster: Int = 0

    val documents: List[Document] = similarityMatrix.getDocuments

    for (doc <- documents) {
      if (!doc.visited) {
        doc.visited = true

        val neighbors: ListBuffer[Document] = ListBuffer() ++= similarityMatrix.getNeighbors(doc, minSim)
        if (neighbors.size < minPoints) {
          doc.clusterType = Some(ClusterType.NOISE)
        }
        else if (numberOfNeighborsWithoutCluster(neighbors.toList) < minPoints) {
          doc.clusterType = Some(ClusterType.EDGE_POINT)
        }
        else {
          currentCluster += 1
          expandCluster(doc, neighbors, currentCluster, minSim, minPoints)
        }
      }
    }
  }

  private def expandCluster(doc: Document, neighbors: ListBuffer[Document], cluster: Int, minSim: Double, minPoints: Int) {
    doc.clusterType = Some(ClusterType.CLUSTER)
    doc.clusterId = Some(cluster)
    for (neighbor <- neighbors) {

      if (!neighbor.visited) {
        neighbor.visited = true

        val content: List[Document] = Try(similarityMatrix.getNeighbors(neighbor, minSim)).toOption.getOrElse(List.empty)
        val neighborsOfNeighbor: ListBuffer[Document] = ListBuffer() ++= content
        if (neighborsOfNeighbor.size >= minPoints) {
          neighbors ++= neighborsOfNeighbor
        }
      }
      if (neighbor.clusterId.isEmpty) {
        neighbor.clusterType = Some(ClusterType.CLUSTER)
        neighbor.clusterId = Some(cluster)
      }
    }
  }

  private def numberOfNeighborsWithoutCluster(neighbors: List[Document]): Int = neighbors.count(n => n.clusterId.isEmpty)
}