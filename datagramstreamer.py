import re

datagram_end_signature = re.compile(b"^![a-fA-F0-9]{4}$")


class DatagramStreamer:
    def __init__(self, stream, max_buffer_length):
        self.stream = stream
        self._datagram_generator = self.getDatagram(max_buffer_length)

    @staticmethod
    def getDatagram(max_buffer):
        buffer = bytearray()
        find_start = 0
        while True:
            match_object = datagram_end_signature.search(buffer, find_start)
            if match_object:
                next_datagram_idx = match_object.start()

                if next_datagram_idx < 0:
                    if len(buffer) > max_buffer:
                        raise ValueError("Buffer too large.")
                    find_start = len(buffer)
                    more_data = yield
                else:
                    datagram = buffer[: next_datagram_idx + 1]
                    del buffer[: next_datagram_idx + 1]
                    find_start = 0
                    more_data = yield datagram

                if more_data is None:
                    buffer += bytes(more_data)

    async def readDatagram(self):
        datagram = next(self._datagram_generator)
        while datagram is None:
            more_data = await self.stream.receive_some(1024)
            if not more_data:
                return b""
            datagram = self._datagram_generator.send(more_data)

        return datagram

    # @staticmethod
    # def getDatagram(max_buffer):
    #     buffer = bytearray()
    #     find_start = 0
    #     while True:
    #         next_datagram_idx = buffer.find("!", find_start)
    #         if next_datagram_idx < 0:
    #             if len(buffer) > max_buffer:
    #                 raise ValueError("Buffer too large.")
    #             find_start = len(buffer)
    #             more_data = yield
    #         else:
    #             datagram = buffer[: next_datagram_idx + 1]
    #             del buffer[: next_datagram_idx + 1]
    #             find_start = 0
    #             more_data = yield datagram

    #         if more_data is None:
    #             buffer += bytes(more_data)
