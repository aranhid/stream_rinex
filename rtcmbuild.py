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
        bits += f"{value:0{datasiz(dfd)}b}"

    # convert bit stream to octets
    octets = [f"0b{bits[i : i + 8]:0<8}" for i in range(0, len(bits), 8)]

    # convert octets to bytes
    pay = b""
    for octet in octets:
        pay += int(octet, 2).to_bytes(1, "little")
    return pay

def utctoweekseconds(utc,leapseconds):
    """ Returns the GPS week, the GPS day, and the seconds 
        and microseconds since the beginning of the GPS week """
    import datetime
    datetimeformat = "%Y-%m-%d %H:%M:%S"
    epoch = datetime.datetime.strptime("1980-01-06 00:00:00",datetimeformat)
    tdiff = utc - epoch  + datetime.timedelta(seconds=(leapseconds - 19))
    gpsweek = tdiff.days // 7 
    gpsdays = tdiff.days - 7 * gpsweek         
    gpsseconds = tdiff.seconds + 86400 * (tdiff.days - 7 * gpsweek)
    return gpsseconds


def create_datafield(df) -> list:
    # https://ge0mlib.com/papers/Protocols/RTCM_SC-104_v3.2.pdf

    sat_id = int(df['Satellite'][1:])

    if not isinstance(df["P range"], dict):
        return None

    timestamp = df['Timestamp'].to_pydatetime()
    gpsseconds = utctoweekseconds(timestamp, 37)
    gpsmsec = gpsseconds * 1000

    delimiter = 299792.46

    df014 =int(df["P range"].get(1) // delimiter)
    L1_pseudorange = df["P range"].get(1) - df014 * delimiter
    L2_preudorange = df["P range"].get(2) - df014 * delimiter

    if L1_pseudorange == 0 or L2_preudorange == 0:
        return None

    # if isinstance(df["Phase"], dict):
    #     L1_phaserange = df["Phase"].get(1) / 1000
    #     L2_phaserange = df["Phase"].get(2) / 1000
    # else:
    #     L1_phaserange = 0
    #     L2_phaserange = 0

    data = [
        # Header
        ("DF002", 1004), # Message Number [UINT12]
        ("DF003", 0), # Reference Station ID [UINT12]
        ("DF004", gpsmsec), # GPS Epoch Time (TOW) [UINT30]
        ("DF005", 0), # Synchronous GNSS Message Flag [BIT1]
        ("DF006", 1), # No. of GPS Satellite Signals Processed [UINT5]
        ("DF007", 0), # GPS Divergencefree Smoothing Indicator [BIT1]
        ("DF008", 0), # GPS Smoothing Interval [BIT3]

        # Signal
        ("DF009", sat_id), # GPS Satellite ID [UINT6]
        ("DF010", 0), # GPS L1 Code Indicator [BIT1]
        ("DF011", int(L1_pseudorange // (datascale('DF011')))), # GPS L1 Pseudorange, in meters [UINT24]
        ("DF012", 0), # GPS L1 PhaseRange - L1 Pseudorange, in meters [INT20], нужно рассчитать phase range
        ("DF013", 0), # GPS L1 Lock Time Indicator [UINT7]
        ("DF014", df014), # GPS Integer L1 Pseudorange Modulus Ambiguity
        ("DF015", 0), # GPS L1 CNR
        ("DF016", 0), # GPS L2 Code Indicator [BIT2]
        ("DF017", int((L2_preudorange - L1_pseudorange) // datascale('DF017'))), # GPS L2-L1 Pseudorange Difference [INT14]
        ("DF018", 0), # GPS L2-L1 Pseudorange Difference, in meters [INT20], нужно рассчитать phase range
        ("DF019", 0), # GPS L2 Lock Time Indicator [UINT7]
        ("DF020", 0) # GPS L2 CNR
    ]

    return data


def create_payload(row) -> bytes:
    datafield = create_datafield(row)
    if datafield is None:
        return None

    payload = df2payload(datafield)
    return payload


if __name__ == '__main__':
    _, df = reader.get_dataframe(["C:\\Users\\vladm\\Documents\\Work\\ISNO_22APR05_225941.22O"], timedelta(seconds=30))
    print(df)
    for index, row in df.iterrows():
        print(row["Satellite"][0])
        if row['Satellite'][0] == 'G':
            payload = create_payload(row)
            if payload is not None:
                msg = RTCMMessage(payload=payload)
                print(f"\nmessage = {msg}")
