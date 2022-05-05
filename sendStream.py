#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import base64
import json
from math import nan
import math
import sys
import time
from datetime import timedelta
from dateutil.parser import parse
from confluent_kafka import Producer
import socket

from reader import get_dataframe
from rtcmbuild import create_payload


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

    timestamps = working_df['Timestamp'].unique()
    # print(timestamps)

    for timestamp in timestamps:
        # print(timestamp)
        time_df = working_df[working_df['Timestamp'] == timestamp]
        # print(time_df)
        for index, row in time_df.iterrows():
            if row['Satellite'][0] == 'G':
                print(row)
                if not math.isnan(row['P range tec']):
                    payload = create_payload(row)

                    debug_json = {}
                    debug_json['Timestamp'] = str(row['Timestamp'].to_pydatetime())
                    debug_json['Satellite'] = row['Satellite']
                    debug_json['P range tec'] = row['P range tec']
                    debug_json['P range 1'] = row['P range'].get(1)
                    debug_json['P range 2'] = row['P range'].get(2)
                    debug_json['bin_message'] =  base64.b64encode(payload).decode("utf8")
                
                    jresult = json.dumps(debug_json)

                    if payload is not None:
                        producer.produce(topic, key=p_key, value=jresult, callback=acked)
                    
        producer.flush()
        time.sleep(interval.total_seconds())

         # # line = working_df.loc[index]
            # timestamp, value = row['Timestamp'], payload
            # timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            # # Convert csv columns to key value pair
            # result = {}
            # result[timestamp] = value
            # # Convert dict to json as message format
            # jresult = json.dumps(result)

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
