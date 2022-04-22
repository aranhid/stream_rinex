#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from datetime import timedelta
from dateutil.parser import parse
from confluent_kafka import Producer
import socket

from reader import get_dataframe


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('files', type=str, nargs='+', help='path to RINEX file')
    parser.add_argument('interval', type=float,
                            help='Time series csv file.')

    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.files[0]

    conf = {'bootstrap.servers': "localhost:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    interval = timedelta(seconds=args.interval)
    common_gaps_df, working_df = get_dataframe(args.files, interval)

    # rdr = csv.reader(open(args.filename))
    # next(rdr)  # Skip header
    index = 0

    for index, row in working_df.iterrows():
        # line = working_df.loc[index]
        timestamp, value = row['Timestamp'], float(row['Phase tec'])
        timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        # Convert csv columns to key value pair
        result = {}
        result[timestamp] = value
        # Convert dict to json as message format
        jresult = json.dumps(result)

        producer.produce(topic, key=p_key, value=jresult, callback=acked)
        producer.flush()
        time.sleep(interval.total_seconds())

    # while True:

    #     try:
    #         line = working_df.loc[index]
    #         index += 1
    #         timestamp, value = line['Timestamp'], float(line['Phase tec'])
    #         # Convert csv columns to key value pair
    #         result = {}
    #         result[timestamp] = value
    #         # Convert dict to json as message format
    #         jresult = json.dumps(result)

    #         producer.produce(topic, key=p_key, value=jresult, callback=acked)
    #         producer.flush()
    #         time.sleep(interval.total_seconds())


    #     except TypeError:
    #         sys.exit()


if __name__ == "__main__":
    main()
