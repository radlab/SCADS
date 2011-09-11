package edu.berkeley.cs
package twitter

import avro.marker._

case class User() extends AvroRecord {
  var contributors_enabled: Boolean = false
  var screen_name: String = null
  var lang: Option[String] = None
  var location: Option[String] = None
  var following: Option[String] = None
  var profile_sidebar_border_color: String = null
  var verified: Boolean = false
  var followers_count: Long = 0L
  var description: Option[String] = None
  var friends_count: Long = 0L
  var geo_enabled: Boolean = false
  var url: Option[String] = None
  var favourites_count: Long = 0L
  var protected_tweet: Option[Boolean] = None
  var time_zone: Option[String] = None
  var name: String = null
  var statuses_count: Long = 0L
  var profile_image_url: String = null
  var id: Long = 0L
  var utc_offset: Option[Long] = None
  var created_at: String = null
  var profile_link_color: String = null
  var profile_sidebar_fill_color: String = null
  var profile_background_tile: Boolean = false
  var profile_background_image_url: String = null
  var profile_background_color: String = null
  var notifications: Option[String] = None
  var profile_text_color: String = null
  var `protected`: Boolean = false
}

case class Geo(var `type`: String, var coordinates: List[Double]) extends AvroRecord

case class Tweet() extends AvroRecord {
  var contributors: Option[String] = None
  var text: String = null
  var source: String = null
  var geo: Option[Geo] = null
  var truncated: Boolean = false
  var favorited: Boolean = false
  var user: User = null
  var in_reply_to_user_id: Option[Long] = None
  var in_reply_to_status_id: Option[Long] = None
  var id: Long = 0L
  var in_reply_to_screen_name: Option[String] = None
  var created_at: String = null
  var retweeted_status: Option[Tweet] = None
}
