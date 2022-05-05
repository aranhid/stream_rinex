#!/usr/bin/env python

"""Consumes stream for printing all messages to the console.
"""

import argparse
from datetime import datetime, timedelta
import json
import base64
import sys
import socket
from pyrtcm import RTCMMessage
from confluent_kafka import Consumer, KafkaError, KafkaException
from gnss_tec import tec


def gpsmsectotime(msec,leapseconds) -> datetime:
    datetimeformat = "%Y-%m-%d %H:%M:%S"
    epoch = datetime.strptime("1980-01-06 00:00:00",datetimeformat)
    tdiff = datetime.utcnow() - epoch  + timedelta(seconds=(leapseconds - 19))
    gpsweek = tdiff.days // 7 
    t = epoch + timedelta(weeks=gpsweek) + timedelta(milliseconds=msec) - timedelta(seconds=(leapseconds - 19))
    return t


def msg_process(msg):
    val = msg.value()
    dval = json.loads(val)

    base64_payload = dval['bin_message']
    rtcm_payload = base64.b64decode(base64_payload.encode('utf-8'))
    rtcm_msg = RTCMMessage(payload=rtcm_payload)

    if rtcm_msg.identity == '1004':
        delimiter = 299792.46

        sat_id = rtcm_msg.DF009_01
        sat = f'G{sat_id:02}'

        p_range_1 = rtcm_msg.DF011_01 + delimiter * rtcm_msg.DF014_01
        p_range_2 = p_range_1 + rtcm_msg.DF017_01
        
        t = tec.Tec(datetime.now(), 'GPS', sat)
        t.p_range = {1: p_range_1, 2: p_range_2}
        t.p_range_code = {1: 'C1C', 2: 'C2W'}
        p_range_tec = t.p_range_tec

        print(sat)
        print(f'timestamp = {rtcm_msg.DF004}')
        print(f'time = {gpsmsectotime(rtcm_msg.DF004, 37)}')
        print(f'P range 1: original = {dval["P range 1"]}, rtcm = {p_range_1}')
        print(f'P range 2: original = {dval["P range 2"]}, rtcm = {p_range_2}')
        print(f'P range tec: original = {dval["P range tec"]}, rtcm = {p_range_tec}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()
