#!/usr/bin/env python

"""Generates a stream to Kafka from a RINEX file.
"""

import math
import time
import socket
import argparse
import numpy as np
from confluent_kafka import Producer
from datetime import datetime, timedelta

from reader import get_dataframe
from rtcmbuild import create_payload


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('files', type=str, nargs='+', help='path to RINEX file.')
    parser.add_argument('interval', type=float, help='interval of RINEX file, in seconds.')
    parser.add_argument('host', type=str, help='Host of the Kafka broker')
    parser.add_argument('topic', type=str, help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False, help='Speed up time series by a given multiplicative factor.')
    parser.add_argument('--from-current-time', action='store_true', help='Send stream from current time.')
    args = parser.parse_args()

    interval = timedelta(seconds=args.interval)
    host = args.host
    topic = args.topic
    p_key = args.files[0]
    speed = args.speed
    from_current_time = args.from_current_time

    try:
        conf = {'bootstrap.servers': f"{host}:9092",
                'client.id': socket.gethostname()}
        producer = Producer(conf)

        working_df = get_dataframe(args.files, interval)

        timestamps = np.unique(working_df['Timestamp'].dt.to_pydatetime())

        if from_current_time:
            current_gps_time = datetime.utcnow()
            timestamps = [ts for ts in timestamps if ts.time() >= current_gps_time.time()]
            sec_to_sleep = timedelta(hours=timestamps[0].time().hour - current_gps_time.time().hour,
                                    minutes=timestamps[0].time().minute - current_gps_time.time().minute,
                                    seconds=timestamps[0].time().second - current_gps_time.time().second).seconds
            time.sleep(sec_to_sleep)

        for timestamp in timestamps:
            print(timestamp)
            time_df = working_df[working_df['Timestamp'] == timestamp]
            for index, row in time_df.iterrows():
                if row['Satellite'][0] == 'G':
                    if not math.isnan(row['P range tec']) and not math.isnan(row['Phase tec']):
                        payload = create_payload(row)
                        if payload is not None:
                            producer.produce(topic, key=p_key, value=payload, callback=acked)
                        
            producer.flush()
            time.sleep(interval.total_seconds() / speed)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
