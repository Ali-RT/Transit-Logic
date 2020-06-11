#!/usr/bin/env python3
"""Skip Kafka Producer
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.1'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

from dataclasses import asdict, dataclass
import time
from confluent_kafka import avro
from pathlib import Path
from producer import Producer
import logging.config

logging.config.fileConfig("log.ini")
logger = logging.getLogger('skip-producer-logger')

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BROKER_URL = "PLAINTEXT://localhost:9092"
SOURCE_FILE = 'Skip.txt'

delivered_records = 0


@dataclass
class LocationEvent:
    id: str
    is_disabled: int
    is_reserved: int
    last_updated: int
    lat: float
    lon: float
    operator: str
    type: str


def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        logger.error(f"Failed to deliver message: {err}")
    else:
        delivered_records += 1
        logger.info(f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}.")


class Skip(Producer):

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/skip_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/skip_value.json")

    def __init__(self):
        topic_name = 'insight.project.skip'
        super().__init__(
            topic_name,
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            num_partitions=1,
            num_replicas=1,
        )

    def run(self):
        """Simulates location events"""
        while True:
            with open(SOURCE_FILE) as f:
                for line in f:
                    sl = line.rstrip().split(",")
                    self.producer.produce(topic=self.topic_name,
                                          key={"timestamp": self.time_millis},
                                          value=asdict(
                                              LocationEvent(sl[0], int(sl[1]), int(sl[2]), int(sl[3]), float(sl[4]),
                                                            float(sl[5]), sl[6], sl[7])), on_delivery=acked)
                    time.sleep(0.1)  # Creating some delay to allow proper rendering of the cab locations on the map

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush(timeout=5)


def main():
    try:
        obj = Skip()
        obj.run()
    except KeyboardInterrupt as e:
        logger.error(e)
        logger.info("shutting down")


if __name__ == "__main__":
    main()
