package net.gutefrage.example.clustering.model

import net.gutefrage.example.clustering.model.ClusterType.ClusterType

class Document(val id: String, var visited: Boolean = false, var clusterType: Option[ClusterType] = None, var clusterId: Option[Int] = None) {

  override def toString: String = {
    s"Document(id=$id, clusterId=$clusterId, clusterType=$clusterType)"
  }
}

object ClusterType extends Enumeration {
  type ClusterType = Value
  val CLUSTER, NOISE, EDGE_POINT = Value
}
