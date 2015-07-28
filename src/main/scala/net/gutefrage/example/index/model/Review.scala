package net.gutefrage.example.index.model

case class Review(review_id: String,
                   business_id: String,
                   user_id: String,
                   stars: Double,
                   text: String,
                   date: String,
                   votes: Map[String, Int])


