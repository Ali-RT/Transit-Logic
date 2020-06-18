#!/usr/bin/env python3
"""
    Kafka Producer
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.2'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

from dataclasses import asdict, dataclass
import time
from confluent_kafka import avro
from pathlib import Path
import logging.config
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
import logging.config


SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"
OPERATOR = "skip"
SOURCE_FILE = f"{OPERATOR}.txt"
TOPIC_NAME = f"com.insight.project.{OPERATOR}.producer"

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

delivered_records = 0


@dataclass
class Event(object):
    id: str
    is_disabled: int
    is_reserved: int
    last_updated: int
    lat: float
    lon: float
    operator: str
    type: str
    geo7: str = ''
    geo8: str = ''
    geo9: str = ''
    timestamp: str = ''


def acked(err, msg):
    """
    Reports the failure or success of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """
    global delivered_records
    if err is not None:
        logger.error(f"Failed to deliver message: {err}")
    else:
        delivered_records += 1
        logger.info(f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}.")


def topic_exists(client, topic_name):
    """
    Reports if the topic is created
    Args:
        client (Kafka Client): The Kafka admin client
        topic_name (str): the topic name to be checked
    """
    topic_data = client.list_topics(timeout=2)
    return topic_name in set(t.topic for t in iter(topic_data.topics.values()))


def create_topic(client, topic_name):
    """
    Creates a Kafka topic
    Args:
        client (Kafka Client): The Kafka admin client
        topic_name (str): the topic to be created
    """
    futures = client.create_topics(
        [
            NewTopic(
                topic=topic_name,
                num_partitions=5,
                replication_factor=1,
                config={
                    "cleanup.policy": "delete",
                    "delete.retention.ms": "2000",
                    "file.delete.delay.ms": "2000",
                },
            )
        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
            logger.info("topic created")
        except Exception as e:
            logger.error(f"failed to create topic {topic_name}: {e}")


def time_millis():
    """
    Creates keys for Kafka events
    """
    return int(round(time.time() * 1000))


def kafka_producer(topic_name):
    """
    Kafka Avro Producer, produces events given schema
    """
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/{OPERATOR}_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/{OPERATOR}_value.json")

    broker_properties = {
        "bootstrap.servers": BROKER_URL,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "client.id": "base.producer",
    }

    producer = AvroProducer(broker_properties,
                            default_key_schema=key_schema,
                            default_value_schema=value_schema,
                            )

    while True:
        try:
            with open(SOURCE_FILE) as f:
                while True:
                    line = f.readline()
                    if line:
                        sl = line.rstrip().split(",")
                        producer.produce(topic=topic_name,
                                         key={"timestamp": time_millis()},
                                         value=asdict(Event(sl[0], int(sl[1]), int(sl[2]), int(sl[3]), float(sl[4]),
                                                            float(sl[5]), sl[6], sl[7])),
                                         on_delivery=acked)
                        time.sleep(0.1)
                    else:
                        pass
        except KeyboardInterrupt:
            break
        except ValueError:
            logger.error("Invalid input, discarding record...")

    producer.flush(timeout=1)


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    exists = topic_exists(client, TOPIC_NAME)
    logger.info(f"Topic {TOPIC_NAME} exists: {exists}")

    if exists is False:
        create_topic(client, TOPIC_NAME)
    try:
        kafka_producer(TOPIC_NAME)
    except KeyboardInterrupt as e:
        logger.error(e)
        logger.info("shutting down")


if __name__ == "__main__":
    main()
