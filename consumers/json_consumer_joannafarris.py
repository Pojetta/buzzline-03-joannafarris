"""
json_consumer_joannafarris.py

Consume json messages from a Kafka topic and process them.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting author occurrences

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("BUZZ_TOPIC", "unknown_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up Data Store to hold author counts
#####################################

# Initialize a dictionary to store author counts
# The defaultdict type initializes counts to 0
# pass in the int function as the default_factory
# to ensure counts are integers
# {author: count} author is the key and count is the value
author_counts: defaultdict[str, int] = defaultdict(int)
TOTAL_MESSAGES: int = 0
ALERT_KEYWORDS = {"kafka", "error", "fail"}  # simple pattern match on message text


#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka.
    Real-time analytics:
      - count messages per author
      - alert on keyword match in 'message'
      - alert on missing/unknown author
      - milestone alert when an author's count hits 5, 10, ...
      - print a short summary every 10 total messages
    """
    from typing import Any
    global TOTAL_MESSAGES

    try:
        logger.debug(f"Raw message: {message}")

        # Deserialize JSON string -> dict
        message_dict: dict[str, Any] = json.loads(message)
        logger.info(f"Processed JSON message: {message_dict}")

        # Extract fields with safe defaults
        text = str(message_dict.get("message", "")).strip()
        author = str(message_dict.get("author", "unknown")).strip().lower() or "unknown"

        # Update counters
        author_counts[author] += 1
        TOTAL_MESSAGES += 1

        # ------- Alerts (simple, real-world-ish) -------
        # 1) Keyword alert on message text (e.g., "kafka", "error", "fail")
        if any(k in text.lower() for k in ALERT_KEYWORDS):
            logger.warning(f"ALERT: keyword match in message (author={author}): {text}")

        # 2) Author missing/unknown
        if author == "unknown":
            logger.warning("ALERT: message missing author")

        # 3) Milestone per author (5, 10, 15, …)
        if author_counts[author] % 5 == 0:
            logger.info(f"Milestone: '{author}' reached {author_counts[author]} messages")

        # Log current counts
        logger.info(f"Author counts: {dict(author_counts)}")

        # 4) Brief summary every 10 total messages
        if TOTAL_MESSAGES % 10 == 0:
            top = sorted(author_counts.items(), key=lambda x: x[1], reverse=True)[:3]
            summary = ", ".join(f"{a}:{c}" for a, c in top) or "no data"
            logger.info(f"Summary after {TOTAL_MESSAGES} msgs | top authors: {summary}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Performs analytics on messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        while True:
            # poll returns a dict: {TopicPartition: [ConsumerRecord, ...], ...}
            records = consumer.poll(timeout_ms=1000, max_records=100)
            if not records:
                continue

            for _tp, batch in records.items():
                for msg in batch:
                    # value_deserializer in utils_consumer already decoded this to str
                    message_str: str = msg.value
                    logger.debug(f"Received message at offset {msg.offset}: {message_str}")
                    process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        
    logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

# Ensures this script runs only when executed directly (not when imported as a module).
if __name__ == "__main__":
    main()
