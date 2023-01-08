import trio
from trio_serial import SerialStream

from datagramstreamer import DatagramStreamer

import array

import serial

# import random
# import time

# from datetime import timedelta
from datetime import datetime
import pytz

from influxdb_client import InfluxDBClient  # , Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


class CRC16:
    def __init__(self, poly=0xA001):
        self.poly = poly
        self.table = array.array("H")

        for i in range(256):
            value = 0
            tmp = i
            for j in range(8):
                if ((value ^ tmp) & 0x0001) != 0:
                    value = (value >> 1) ^ self.poly
                else:
                    value = value >> 1
                tmp = tmp >> 1

            self.table.append(value)

    def crc(self, data: bytes):
        crc = 0
        for b in data:
            index = (crc ^ b) & 0xFF
            crc = (crc >> 8) ^ self.table[index]

        return crc


crc16 = CRC16()


def get_influxdb_write_api():
    client = InfluxDBClient.from_config_file("config.ini")
    write_api = client.write_api(write_options=SYNCHRONOUS)

    return write_api


def decode_telegram_line(line):
    blocks = line.split(b"(")
    if len(blocks) <= 1:
        return None, None

    obis = blocks[0].decode("UTF-8")
    data = []

    for block in blocks[1:]:
        data.append(block[:-1].decode("UTF-8"))

    return obis, data


def telegram_lines(telegram):
    for line in telegram.split(b"\r\n"):
        yield line


def telegram_to_dict(telegram, crc):
    r = {}
    for line in telegram_lines(telegram):
        obis, data = decode_telegram_line(line)

        if obis:
            r[obis] = data
    return r


async def read_telegrams(send_channel):
    buf = bytearray(1024)

    async with SerialStream("/dev/ttyUSB0", baudrate=115200) as ser:
        async with send_channel:
            start = -1
            end = -1
            while True:
                buf = buf + await ser.receive_some(512)

                if start == -1:
                    start = buf.find(b"/")

                if end == -1:
                    end = buf.find(b"!")

                if start >= 0 and end >= 0 and len(buf) - end > 4:
                    telegram = buf[start : end + 1]
                    crc = int(b"0x" + buf[end + 1 : end + 5], 16)
                    if crc16.crc(telegram) == crc:
                        data = telegram_to_dict(telegram, crc)
                        await send_channel.send(data)
                    else:
                        print("checksum error")
                    buf = buf[end + 5 :]
                    start = end = -1


def convert_timestamp_to_unix_epoch_in_s(raw_ts):
    local_tz = pytz.timezone("Europe/Amsterdam")
    ts = datetime.strptime(raw_ts[:-1], "%y%m%d%H%M%S")
    dst = False
    if raw_ts[-1:] == "S":
        dst = True
    local_time = local_tz.localize(ts, dst)
    utc = local_time.astimezone(pytz.utc)

    return int(
        (utc - datetime(1970, 1, 1, tzinfo=pytz.timezone("utc"))).total_seconds()
    )


def get_phase_voltages(telegram):
    # data = {}
    # data["V_L1"] = telegram["1-0:32.7.0"][0][:-2]
    # data["V_L2"] = telegram["1-0:52.7.0"][0][:-2]
    # data["V_L3"] = telegram["1-0:72.7.0"][0][:-2]

    # return data

    return {
        "V_L1": telegram["1-0:32.7.0"][0][:-2],
        "V_L2": telegram["1-0:52.7.0"][0][:-2],
        "V_L3": telegram["1-0:72.7.0"][0][:-2],
    }


def get_phase_amperes(telegram):
    # data = {}
    # data["A_L1"] = telegram["1-0:31.7.0"][0][:-2]
    # data["A_L2"] = telegram["1-0:51.7.0"][0][:-2]
    # data["A_L3"] = telegram["1-0:71.7.0"][0][:-2]

    return {
        "A_L1": telegram["1-0:31.7.0"][0][:-2],
        "A_L2": telegram["1-0:51.7.0"][0][:-2],
        "A_L3": telegram["1-0:71.7.0"][0][:-2],
    }


def get_actual_power(telegram):
    # data = {}
    # data["+P"] = telegram["1-0:1.7.0"][0][:-3]
    # data["-P"] = telegram["1-0:2.7.0"][0][:-3]

    # return data

    return {"+P": telegram["1-0:1.7.0"][0][:-3], "-P": telegram["1-0:2.7.0"][0][:-3]}


def get_deliverd_electricity(telegram):
    # data = {}
    # data["+PT1"] = telegram["1-0:1.8.1"][0][:-4]
    # data["+PT2"] = telegram["1-0:1.8.2"][0][:-4]
    # data["-PT1"] = telegram["1-0:2.8.1"][0][:-4]
    # data["-PT2"] = telegram["1-0:2.8.2"][0][:-4]

    # return data

    return {
        "+PT1": telegram["1-0:1.8.1"][0][:-4],
        "+PT2": telegram["1-0:1.8.2"][0][:-4],
        "-PT1": telegram["1-0:2.8.1"][0][:-4],
        "-PT2": telegram["1-0:2.8.2"][0][:-4],
    }


def get_actual_usage(telegram):
    # data = {}
    # data["L1+P"] = telegram["1-0:21.7.0"][0][:-3]
    # data["L2+P"] = telegram["1-0:41.7.0"][0][:-3]
    # data["L3+P"] = telegram["1-0:61.7.0"][0][:-3]
    # data["L1-P"] = telegram["1-0:22.7.0"][0][:-3]
    # data["L2-P"] = telegram["1-0:42.7.0"][0][:-3]
    # data["L3-P"] = telegram["1-0:62.7.0"][0][:-3]

    # return data

    return {
        "L1+P": telegram["1-0:21.7.0"][0][:-3],
        "L2+P": telegram["1-0:41.7.0"][0][:-3],
        "L3+P": telegram["1-0:61.7.0"][0][:-3],
        "L1-P": telegram["1-0:22.7.0"][0][:-3],
        "L2-P": telegram["1-0:42.7.0"][0][:-3],
        "L3-P": telegram["1-0:62.7.0"][0][:-3],
    }


def get_phase_telegram_fields(telegram):
    volts = get_phase_voltages(telegram)
    amperes = get_phase_amperes(telegram)
    usage = get_actual_usage(telegram)
    power = get_actual_power(telegram)
    delivered = get_deliverd_electricity(telegram)

    fields = generate_line({**volts, **amperes, **usage, **power, **delivered})

    return fields


def generate_line(data_points):
    fields = None

    for k, v in data_points.items():
        if fields:
            fields = f"{fields},{k}={v}"
        else:
            fields = f"{k}={v}"

    return fields


async def write_influx_data(receive_channel, write_api):
    async with receive_channel:
        while True:
            try:
                async for telegram in receive_channel:
                    raw_ts = telegram["0-0:1.0.0"][0]
                    seconds_sinds_unix_epoch = convert_timestamp_to_unix_epoch_in_s(
                        telegram["0-0:1.0.0"][0]
                    )

                    fields = get_phase_telegram_fields(telegram)
                    line = f"telegram {fields} {seconds_sinds_unix_epoch}000000000"
                    # print(line)
                    write_api.write("smartmeter", "home", line)
            except:
                await trio.sleep(1)


async def main():
    ser = serial.Serial("/dev/ttyUSB0")
    datastreamer = DatagramStreamer(ser, 12)
    dg = datastreamer.readDatagram()
    print(dg)
    return
    write_api = get_influxdb_write_api()

    async with trio.open_nursery() as nursery:
        send_channel, receive_channel = trio.open_memory_channel(0)
        nursery.start_soon(read_telegrams, send_channel)
        nursery.start_soon(write_influx_data, receive_channel, write_api)


if __name__ == "__main__":
    trio.run(main)
