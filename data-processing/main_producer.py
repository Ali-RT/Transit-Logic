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

import argparse
import distutils.util
from dataclasses import asdict, dataclass
from confluent_kafka import avro
from pathlib import Path
import logging.config
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
import logging.config
import time
import boto3
import codecs


logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)
delivered_records = 0


@dataclass
class Event(object):
    id: str = ''
    is_disabled: int = 0
    is_reserved: int = 0
    last_updated: int = 0
    lat: float = 0.00
    lon: float = 0.00
    operator: str = ''
    type: str = ''
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


def kafka_producer(topic_name, BROKER_URL, SCHEMA_REGISTRY_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME):
    """
    Kafka Avro Producer, produces events given schema
    """
    # Avro schema
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/key_schema.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/value_schema.json")
    # Get a handle on s3
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    # s3_object = s3.Object(bucket_name=S3_BUCKET_NAME, key=f'{OPERATOR}.txt')
    s3_object = s3.Object(bucket_name=S3_BUCKET_NAME, key='Bird.txt')
    streaming_body = s3_object.get()['Body']

    broker_properties = {
        "bootstrap.servers": BROKER_URL,
        "schema.registry.url": SCHEMA_REGISTRY_URL,
        "client.id": "base.producer",
    }

    producer = AvroProducer(broker_properties,
                            default_key_schema=key_schema,
                            default_value_schema=value_schema,
                            )

    keys = ['bike_id', 'is_disabled', 'is_reserved', 'last_updated', 'lat', 'lon', 'operator', 'vehicle_type']
    while True:
        try:
            for ln in codecs.getreader('utf-8')(streaming_body):
                # sl = ln.rstrip().split(",")
                d = dict((x.strip(), y.strip())
                         for x, y in (element.split(':')
                                      for element in ln.split(', ')))
                print(d)
                if set(keys).issubset((d.keys())):
                    producer.produce(topic=topic_name,
                                     key={"timestamp": time_millis()},
                                     value=asdict(Event(d['bike_id'], distutils.util.strtobool(d['is_disabled']), distutils.util.strtobool(d['is_reserved']),
                                                        int(d['last_updated']), float(d['lat']), float(d['lon']),
                                                        d['operator'], d['vehicle_type'])),
                                     on_delivery=acked)
                time.sleep(2)

        except KeyboardInterrupt:
            break

    producer.flush(timeout=1)


def main(args):
    # SCHEMA_REGISTRY_URL = "http://localhost:8081"
    SCHEMA_REGISTRY_URL = args.schema_registry
    # BROKER_URL = "PLAINTEXT://localhost:9092"
    BROKER_URL = args.bootstrap_servers
    # OPERATOR = "bird"
    OPERATOR = args.operator
    # Topic name
    TOPIC_NAME = f"com.insight.project.{OPERATOR}.producer"

    # SOURCE_FILE = f"/home/bison/Downloads/Data/Sample/{OPERATOR}.txt"
    # AWS S3
    S3_BUCKET_NAME = "insight.micromobility.sample.data"
    AWS_ACCESS_KEY_ID = "AKIAJXKNBSMCPHSNPTZQ"
    AWS_SECRET_ACCESS_KEY = "V9AGNwoUbURRh607MNtgjNKiRYp6Nbeb6rugzEwb"

    client = AdminClient({"bootstrap.servers": BROKER_URL})

    exists = topic_exists(client, TOPIC_NAME)
    logger.info(f"Topic {TOPIC_NAME} exists: {exists}")

    if exists is False:
        create_topic(client, TOPIC_NAME)
    try:
        kafka_producer(TOPIC_NAME, BROKER_URL, SCHEMA_REGISTRY_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME)
    except KeyboardInterrupt as e:
        logger.error(e)
        logger.info("shutting down")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-o', dest="operator", required=True,
                        help="Operator name")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")

    main(parser.parse_args())
