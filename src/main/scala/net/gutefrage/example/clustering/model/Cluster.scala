package net.gutefrage.example.clustering.model

case class User(name: String, docs: Int)

case class Cluster(id: Int, size: Int)

case class ClusterInfo(id: String, size: Int, topUsers: List[User], avgRating: Double)
