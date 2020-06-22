#!/usr/bin/env python3
"""Faust Streamer
 it streams every single event and add/change some attributes
"""
__author__ = 'Ali Rahim-Taleqani'
__copyright__ = 'Copyright 2020, The Insight Data Engineering'
__credits__ = [""]
__version__ = '0.1'
__maintainer__ = 'Ali Rahim-Taleqani'
__email__ = 'ali.rahim.taleani@gmail.com'
__status__ = 'Development'

import argparse
from dataclasses import dataclass
import faust
import geohash
import datetime
import logging.config
from faust import Worker


logging.config.fileConfig('logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)


@dataclass
class LocationEvent(faust.Record, serializer="json"):
    id: str
    is_disabled: int
    is_reserved: int
    last_updated: int
    lat: float
    lon: float
    operator: str
    type: str
    geo7: str
    geo8: str
    geo9: str
    timestamp: str


def attribute_convertor(e):
    """
    Extracts and add geohash and timestamp to an event
    Args:
        e (Fasut event): a Faust stream event
    """
    e.geo7 = geohash.encode(float(e.lat), float(e.lon), 7)
    e.geo8 = geohash.encode(float(e.lat), float(e.lon), 8)
    e.geo9 = geohash.encode(float(e.lat), float(e.lon), 9)
    e.timestamp = datetime.datetime.fromtimestamp(int(e.last_updated)).strftime('%Y-%m-%d %H:%M:%S')
    return e


def main(args):
    STREAMER_NAME = "main.streamer"
    # BROKER_URL = "kafka://localhost:9092"
    BROKER_URL = args.bootstrap_seervers
    # OPERATOR = "lyft"
    OPERATOR = args.operator

    INCOMING_TOPIC = f"com.insight.project.{OPERATOR}.converter"
    OUTGOING_TOPIC = f"com.insight.project.{OPERATOR}.streaming"
    app = faust.App(STREAMER_NAME, broker=BROKER_URL)

    incoming_topic = app.topic(INCOMING_TOPIC, value_type=LocationEvent)
    outgoing_topic = app.topic(OUTGOING_TOPIC, value_type=LocationEvent)

    @app.agent(incoming_topic)
    async def event(events):
        events.add_processor(attribute_convertor)
        async for le in events:
            await outgoing_topic.send(value=le)

    worker = Worker(app)
    worker.execute_from_commandline()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fasut Streamer")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-o', dest="operator", required=True,
                        help="Operator name")

    main(parser.parse_args())
