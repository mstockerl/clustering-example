package net.gutefrage.example.clustering.algorithm

import net.gutefrage.example.clustering.Constants
import net.gutefrage.example.clustering.model.SimilarDocument
import net.gutefrage.example.index.model.ElasticsearchReviewType
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}

import scala.annotation.tailrec

class SimilarityMatrixBuilder(client: Client, indexName: String) {

  val size = 10000L

  def build(): SimilarityMatrix = {
    val scrollId = getScrollId

    val questions = fetchIds(scrollId, List.empty)
    val questionGroups = questions.grouped(10).toList

    val result = questionGroups.flatMap { questionGroup =>
      try {
        getResult(questionGroup)
      } catch {
        case e: Exception =>
          Thread.sleep(3000)
          getResult(questionGroup)
      }
    }

    SimilarityMatrix(result)
  }

  private def getScrollId: String = {
    client.prepareSearch(indexName)
      .setTypes(ElasticsearchReviewType.name)
      .setSearchType(SearchType.SCAN)
      .setNoFields()
      .setScroll(TimeValue.timeValueMinutes(1))
      .setQuery(QueryBuilders.matchAllQuery())
      .setSize(size.toInt)
      .get().getScrollId
  }

  @tailrec
  private def fetchIds(scrollId: String, reviews: List[String]): List[String] = {
    val response = client.prepareSearchScroll(scrollId)
      .setScroll(TimeValue.timeValueMinutes(1))
      .get()

    val hits = response.getHits
    val newScrollId = response.getScrollId

    println(s"Batch took ${response.getTookInMillis}")
    val newReviews = hits.getHits.map(
      hit => hit.getId
    )

    if (newReviews.length < size) {
      reviews
    } else {
      fetchIds(newScrollId, reviews ++ newReviews)
    }
  }


  private def getResult(questionList: List[String]): List[(String, List[SimilarDocument])] = {
    val multiSearch = client.prepareMultiSearch()

    questionList.map { question =>
      multiSearch.add(client.prepareSearch(indexName)
        .setTypes(ElasticsearchReviewType.name)
        .setNoFields()
        .setQuery(mltQuery(question))
        .setMinScore(Constants.mltMinScore))
    }

    val multiSearchResponse = multiSearch.get()
    val multiSearchSimilarQuestions = multiSearchResponse.getResponses.flatMap {
      case r if !r.isFailure => Some(r.getResponse.getHits.hits().toList.map(h => SimilarDocument(h.getId, h.getScore)))
      case f if f.isFailure =>
        println("Failure: " + f)
        None
    }

    questionList.zip(multiSearchSimilarQuestions)
  }


  private def mltQuery(documentId: String): QueryBuilder = {
    QueryBuilders.moreLikeThisQuery("text")
      .ids(documentId)
      .percentTermsToMatch(0.51f)
      .minTermFreq(1)
      .maxQueryTerms(12)
      .maxDocFreq(50000)
      .minWordLength(2)
      .include(false)
  }
}
