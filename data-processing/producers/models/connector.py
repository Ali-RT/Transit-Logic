#!/usr/bin/env python3
"""Kafka connector
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.1'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

import json
import requests
import logging.config

OPERATOR = "skip"
KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = f"{OPERATOR}_conn"
TOPICS = f"com.insight.project.{OPERATOR}.sink"

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def configure_connector():
    """Calls Kafka Connect to create the Connector"""
    logger.info("creating or updating kafka connect connector...")

    rest_method = requests.post
    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        return

    resp = rest_method(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(
            {
                "name": CONNECTOR_NAME,
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "tasks.max": 2,
                    "topics": TOPICS,
                    "table.name.format": "location_events",
                    "connection.url": "jdbc:postgresql://localhost:5432/postgres",
                    "connection.user": "postgres",
                    "connection.password": "project2020",
                    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                    "key.converter.schemas.enable": "fasle",
                    "key.converter.schema.registry.url": "http://localhost:8081",
                    "value.converter": "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schemas.enable": "true",
                    "value.converter.schema.registry.url": "http://localhost:8081",
                    "errors.tolerance": "all",
                    "auto.create": "true",
                    "auto.evolve": "true",
                    "fields.whitelist": "timestamp,type,operator,id,is_disabled,is_reserved,lat,lon,geo7,geo8,geo9"
                },
            }
        ),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        #print(json.dumps(resp.json(), indent=2))
    except:
        logger.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logger.info("connector created successfully.")
    logger.info("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    configure_connector()
