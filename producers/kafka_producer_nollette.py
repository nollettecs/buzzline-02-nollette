"""
kafka_producer_case.py

Produce some streaming banking-related strings and send them to a Kafka topic.
"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import sys
import time

# Import external packages
from dotenv import load_dotenv

# Import functions from local modules
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
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
    topic = os.getenv("KAFKA_TOPIC", "banking_topic")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 1))
    logger.info(f"Message interval: {interval} seconds")
    return interval

#####################################
# Message Generator
#####################################

def generate_messages(producer, topic, interval_secs):
    """
    Generate a stream of banking-related messages and send them to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.
    """
    string_list: list = [
        "Account opened successfully.",
        "Customer made a deposit of $5000.",
        "Loan application approved for $25,000.",
        "Stock purchase order executed for 100 shares of ABC Corp.",
        "Transaction declined due to insufficient funds.",
        "New credit card issued to customer ID 12345.",
        "Customer transferred $300 to savings account.",
        "Interest payment of $45 credited to account.",
        "Mortgage payment of $1,200 received.",
        "Fraud alert triggered on account ID 67890.",
    ]
    try:
        while True:
            for message in string_list:
                logger.info(f"Generated banking message: {message}")
                # Send the string message directly
                producer.send(topic, value=message)  
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")


#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams generated banking message strings to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # Fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send messages
    logger.info(f"Starting message production to topic '{topic}'...")
    generate_messages(producer, topic, interval_secs)

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()