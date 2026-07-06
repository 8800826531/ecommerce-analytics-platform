resource "kafka_topic" "user_activity" {
  name               = "user-activity"
  partitions         = 3
  replication_factor = 3

  config = {
    "retention.ms" = "86400000"
  }
}

resource "kafka_topic" "product_events" {
  name               = "product-events"
  partitions         = 2
  replication_factor = 3

  config = {
    "retention.ms" = "86400000"
  }
}

resource "kafka_topic" "order_events" {
  name               = "order-events"
  partitions         = 2
  replication_factor = 3

  config = {
    "retention.ms" = "86400000"
  }
}

resource "kafka_topic" "dead_letter_queue" {
  name               = "dead-letter-queue"
  partitions         = 1
  replication_factor = 3

  config = {
    "retention.ms" = "604800000"
  }
}