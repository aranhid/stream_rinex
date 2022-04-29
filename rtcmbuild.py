"""
Example illustrating how to generate RTCM3 payloads from
constituent datafields.

Created on 14 Feb 2022

:author: semuadmin
:copyright: SEMU Consulting © 2022
:license: BSD 3-Clause
"""
from datetime import timedelta
import reader
import pandas as pd
from pyrtcm import RTCMMessage, datasiz, datascale


def df2payload(datafields: list) -> bytes:
    """
    Convert list of (datafield, value) tuples to RTCM3 payload.

    :param list datafields: list of (datafield, value) tuples
    :return: payload as bytes
    :rtype: bytes
    """

    # convert (datafield, value) tuples to bit stream
    bits = ""
    for (dfd, val) in datafields:
        value = val
        if value < 0:
            value = value + 2**datasiz(dfd)
        print(f"{value:0{datasiz(dfd)}b}")
        bits += f"{value:0{datasiz(dfd)}b}"
    print(f"\nbitstream = {bits}")

    # convert bit stream to octets
    octets = [f"0b{bits[i : i + 8]:0<8}" for i in range(0, len(bits), 8)]
    print(f"\noctets = {octets}")

    # convert octets to bytes
    pay = b""
    for octet in octets:
        pay += int(octet, 2).to_bytes(1, "little")
    return pay


def create_datafield(df):
    sat_id = int(df['Satellite'][1:])
    if isinstance(df["P range"], dict):
        L1_pseudorange = df["P range"].get(1)
        L2_preudorange = df["P range"].get(2)
    else:
        L1_pseudorange = 0
        L2_preudorange = 0
    if isinstance(df["Phase"], dict):
        L1_phaserange = df["Phase"].get(1)
        L2_phaserange = df["Phase"].get(2)
    else:
        L1_phaserange = 0
        L2_phaserange = 0

    norm_data = [
        ("DF011", L1_pseudorange), # нужно конвертировать в int
        ("DF012", L1_phaserange - L1_pseudorange), # нужно конвертировать в int
        ("DF017", L2_preudorange - L1_pseudorange),
        ("DF018", L2_phaserange - L1_pseudorange),
    ]

    print(norm_data)

    data = [
        ("DF002", 1003),
        ("DF003", 0),
        ("DF004", 1),
        ("DF005", 1),
        ("DF006", 1),
        ("DF007", 0),
        ("DF008", 0),

        ("DF009", sat_id),
        ("DF010", 0),
        ("DF011", int(L1_pseudorange // datascale('DF011'))), # нужно конвертировать в int
        ("DF012", int((L1_phaserange - L1_pseudorange) // datascale('DF012'))), # нужно конвертировать в int
        ("DF013", 0),
        ("DF016", 0),
        ("DF017", int((L2_preudorange - L1_pseudorange) // datascale('DF017'))),
        ("DF018", int((L2_phaserange - L1_pseudorange) // datascale('DF018'))),
        ("DF019", 0),
    ]

    print(data)
    return data


# data = [
#     ("DF002", 1003),
#     ("DF003", 0),
#     ("DF004", 1),
#     ("DF005", 1),
#     ("DF006", 2),
#     ("DF007", 0),
#     ("DF008", 0),

#     ("DF009", 5),
#     ("DF010", 0),
#     ("DF011", 1),
#     ("DF012", 1),
#     ("DF013", 0),
#     ("DF016", 1),
#     ("DF017", 1),
#     ("DF018", 1),
#     ("DF019", 0),

#     ("DF009", 5),
#     ("DF010", 0),
#     ("DF011", 1),
#     ("DF012", 1),
#     ("DF013", 0),
#     ("DF016", 1),
#     ("DF017", 1),
#     ("DF018", 1),
#     ("DF019", 0),
# ]


if __name__ == '__main__':
    _, df = reader.get_dataframe(["C:\\Users\\vladm\\Documents\\Work\\ISNO_22APR05_225941.22O"], timedelta(seconds=30))
    print(df)
    for index, row in df.iterrows():
        print(row["Satellite"][0])
        if row['Satellite'][0] == 'G':
            data = create_datafield(row)
            payload = df2payload(data)
            msg = RTCMMessage(payload=payload)
            print(f"\nmessage = {msg}")
