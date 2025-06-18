import org.apache.spark.sql.types._

object Config {

  val BATCH_SIZE = 2500
  val BOOTSTRAP_SERVER = sys.env.getOrElse("KAFKA_HOST", "localhost:9092")

  val REVIEW_TOPIC: String = "yelp-topic-review"
  val REVIEW_JSON_PATH: String = sys.env.getOrElse("DATASET_PATH", "../yelp_dataset/") + "yelp_academic_dataset_review.json"
  val SAVE_BATCH_STATE_FILE = "tmp/kafka_batch_state.txt"
}
