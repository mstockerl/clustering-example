package net.gutefrage.example.clustering.algorithm

import net.gutefrage.example.clustering.model.{Document, SimilarDocument}

import scala.util.Try

class SimilarityMatrix(documentReference: Map[String, Document], similarityMatrix: Map[String, List[SimilarDocument]], invertedIndex: Map[String, List[String]]) {

  def getNeighbors(doc: Document, minSimilarity: Double): List[Document] = {
    val sims = similarityMatrix(doc.id).map(_.id)
    val rnn = invertedIndex.getOrElse(doc.id, Seq())
    (sims ++ rnn).distinct.collect {
      case simId if getSimilarity(doc.id, simId) > minSimilarity => documentReference(simId)
    }
  }

  private def getSimilarity(questionId1: String, questionId2: String): Double = {
    Try {
      val simFromOne2Two = similarityMatrix(questionId1).find(_.id == questionId2).map(_.score).getOrElse(0.0)
      val simFromTwo2One = similarityMatrix(questionId2).find(_.id == questionId1).map(_.score).getOrElse(0.0)

      (simFromOne2Two + simFromTwo2One) / 2
    }.toOption.getOrElse(0.0)

  }

  def getDocuments: List[Document] = documentReference.values.toList
}

object SimilarityMatrix {

  def apply(similarityMatrix: List[(String, List[SimilarDocument])]): SimilarityMatrix = {
    val invertedIndex = createInvertedIndex(similarityMatrix)
    val documentReference = createDocumentReference(similarityMatrix)

    new SimilarityMatrix(documentReference, similarityMatrix.toMap, invertedIndex)
  }

  private def createDocumentReference(similarityMatrix: List[(String, List[SimilarDocument])]): Map[String, Document] = {
    similarityMatrix.map{
      case (id, _) => id -> new Document(id)
    }.toMap
  }

  private def createInvertedIndex(similarityMatrix: List[(String, List[SimilarDocument])]): Map[String, List[String]] = {
    similarityMatrix
      .flatMap {
        case (questionId, similarQuestions) => similarQuestions.map(q => q.id -> questionId)
      }
      .groupBy(_._1)
      .map {
        case (id, sim) => id -> sim.map(_._2)
      }
  }

}