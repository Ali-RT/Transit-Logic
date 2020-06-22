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

import argparse
import json
import requests
import logging.config

logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


def configure_connector(args):
    # OPERATOR = "lyft"
    OPERATOR = args.operator
    # KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
    KAFKA_CONNECT_URL = args.kafka_connector
    # SCHEMA_REGISTRY_URL = "http://localhost:8081"
    SCHEMA_REGISTRY_URL = args.schema_registry
    # CONNECTION_URL = "http://localhost:9200"
    CONNECTION_URL = args.elastic_url

    CONNECTOR_NAME = f"elastic-{OPERATOR}-conn"
    TOPICS = f"insight-project-table"

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
                    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
                    "tasks.max": 1,
                    "topics": TOPICS,
                    "key.ignore": "true",


                    "connection.url": CONNECTION_URL,
                    "connection.username": "",
                    "connection.password": "project2020",

                    "value.converter": "io.confluent.connect.avro.AvroConverter",
                    "value.converter.schemas.enable": "true",
                    "value.converter.schema.registry.url": SCHEMA_REGISTRY_URL,
                    "type.name": "kafka-connect",
                    "name": CONNECTOR_NAME
                },
            }
        ),
    )
    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
    except ConnectionError as e:
        logger.error(e)
        logger.error(f"failed creating connector: {json.dumps(resp.json(), indent=2)}")
        exit(1)
    logger.info("connector created successfully.")
    logger.info("Use kafka-console-consumer and kafka-topics to see data!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elasticsearch Sink Connector")
    parser.add_argument('-c', dest="kafka_connector", required=True,
                        help="Kafka Connector URL (host[:port])")
    parser.add_argument('-e', dest="elastic_url", required=True,
                        help="Elasticsearch URL (host[:port])")
    parser.add_argument('-o', dest="operator", required=True,
                        help="Operator name")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")

    configure_connector(parser.parse_args())
