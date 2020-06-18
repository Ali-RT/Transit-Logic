#!/usr/bin/env python3
"""Kafka deserializer
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.2'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

import asyncio
import json
from uuid import uuid4
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka import Producer
import logging.config

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"
OPERATOR = "skip"
CONSUME_TOPIC = f"com.insight.project.{OPERATOR}.producer"
PRODUCE_TOPIC = f"com.insight.project.{OPERATOR}.convertor"

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


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


async def convertor(consume_topic, produce_topic):
    """Consumes data from the Kafka Topic"""
    schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    c = AvroConsumer(
        {
            "bootstrap.servers": BROKER_URL,
            "client.id": "project-insight",
            "group.id": "convertor-in-consumer",
            "auto.offset.reset": "earliest",
        },
        schema_registry=schema_registry,
    )
    c.subscribe([consume_topic])

    p = Producer({"bootstrap.servers": BROKER_URL})

    while True:
        message = c.poll(1.0)
        if message is None:
            logger.info("no message received by consumer")
        elif message.error() is not None:
            logger.error(f"error from consumer {message.error()}")
        else:
            try:
                print(message.value())
                p.produce(topic=produce_topic, key=str(uuid4()), value=json.dumps(message.value()))

            except KeyError as e:
                logger.error(f"Failed to unpack message {e}")
        await asyncio.sleep(0.01)


async def consume_produce(con_topic, pro_topic):
    t = asyncio.create_task(convertor(con_topic, pro_topic))
    await t


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})

    exists = topic_exists(client, PRODUCE_TOPIC)
    logger.info(f"Topic {PRODUCE_TOPIC} exists: {exists}")

    if exists is False:
        create_topic(client, PRODUCE_TOPIC)

    try:
        asyncio.run(consume_produce(CONSUME_TOPIC, PRODUCE_TOPIC))
    except KeyboardInterrupt as e:
        logger.error(f"Failed to unpack message {e}")
        logger.info("shutting down")


if __name__ == "__main__":
    main()
