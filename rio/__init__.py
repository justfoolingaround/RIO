import io
import logging
import tempfile
import typing as t
from warnings import warn

import requests

from .utils import ExistingRanges

WHENCE_TYPE = t.Literal[0, 1, 2]


class RemoteIO(io.IOBase):
    def __init__(
        self,
        *args,
        session: t.Optional[requests.Session] = None,
        buffer: t.Optional[io.BytesIO] = None,
        **kwargs,
    ):
        self.session = session or requests.Session()
        self.request_args = args
        self.headers = kwargs.pop("headers", {})
        self.request_kwargs = kwargs
        self.pos = 0
        self.buffer = buffer or tempfile.SpooledTemporaryFile()
        self.existing_ranges = ExistingRanges()
        self.streaming_response = None
        self.server_read = 0
        self.server_full_size = None

        self.logger = logging.getLogger("remoteio")

    def seek(self, offset: int, whence: WHENCE_TYPE = io.SEEK_SET):
        assert whence in (0, 1, 2), "Invalid whence"
        initial = self.pos

        if whence == io.SEEK_SET:
            self.pos = offset
        elif whence == io.SEEK_CUR:
            self.pos += offset
            offset = self.pos
        elif whence == io.SEEK_END:
            total_size = self.attempt_size_resolving()
            if total_size is None:
                raise ValueError("Cannot seek from EOF when the size is unknown")
            self.pos = total_size + offset
            if offset == 0:
                return self.pos
        else:
            raise ValueError("Invalid whence, must be 0, 1 or 2")

        if self.streaming_response is not None:
            if initial == self.pos:
                self.logger.info(
                    f"Already at position {self.pos}; the internal state is constant."
                )
                return self.pos

            hypothetical_range = (self.pos, self.pos + 1)
            if hypothetical_range in self.existing_ranges:
                self.logger.info(
                    f"Expected data at the range {hypothetical_range} is already in the buffer; not opening an unnecessary stream."
                )
                return self.pos

            self.logger.info(
                f"Closing the previous stream at position: {initial}, starting anew at position: {self.pos}"
            )
            self.streaming_response.close()

        byte_range = f"{offset}"

        if whence <= 1:
            byte_range += "-"

        self.streaming_response = self.request_stream(byte_range)
        return self.pos

    def read(self, n: int = -1):
        if self.pos == 0 and not n + 1:
            warn(
                "The RIO object is being read in full which is the worst case for its usage. Using packages that don't try to read the entire file is highly recommended."
            )

        file_size = self.attempt_size_resolving()
        start = self.pos
        end = (self.pos + n) if n + 1 else file_size

        partitions = (
            self.existing_ranges.iter_partition(start, end)
            if file_size is not None
            else [(start, end)]
        )

        chunk = b""
        initial_position = self.pos

        self.logger.info(
            f"Requested data at the range {start}-{end} ({end-start} bytes) from the server."
        )

        for part_start, part_end in partitions:
            self.buffer.seek(part_start)
            partition_n = part_end - part_start if part_end is not None else -1

            if (part_start, part_end) in self.existing_ranges:
                self.logger.info(
                    f"Reading data at the range {part_start}-{part_end} from existing buffer."
                )
                chunk += self.buffer.read(partition_n)
            else:
                self.seek(part_start)
                if self.streaming_response is None:
                    raise RuntimeError("The stream was not open after seeking")

                self.logger.info(
                    f"Reading data at the range {part_start}-{part_end} from remote."
                )
                server_chunk = self.read_server_chunk(partition_n)

                self.server_read += self.buffer.write(server_chunk)
                self.existing_ranges.add(part_start, part_start + len(server_chunk))
                chunk += server_chunk

        self.pos = initial_position + len(chunk)
        return chunk

    def tell(self):
        return self.pos

    def seekable(self):
        return True

    def readable(self):
        return True

    def writable(self):
        return False

    def close(self):
        if self.streaming_response is not None:
            self.streaming_response.close()

        try:
            self.buffer.close()
        except AttributeError:
            pass

        self.session.close()

    def attempt_size_resolving(self):
        if self.server_full_size is not None:
            return self.server_full_size

        if self.streaming_response is None:
            self.streaming_response = self.request_stream("0-")

        if "Content-Range" in self.streaming_response.headers:
            self.server_full_size = int(
                self.streaming_response.headers["Content-Range"].split("/")[1]
            )
            self.logger.info(
                f"Size of the file was resolved via an existing stream: {self.server_full_size}"
            )
            return self.server_full_size

        return None

    def request_stream(self, byte_range):
        self.logger.info(f"Requesting byte range: {byte_range} from the server.")
        response = self.session.request(
            *self.request_args,
            headers=self.headers | {"Range": f"bytes={byte_range}"},
            **self.request_kwargs,
            stream=True,
        )
        return response

    def read_server_chunk(self, partition_n):
        server_chunk = self.streaming_response.raw.read(partition_n)
        size_of_chunk = len(server_chunk)

        if partition_n != -1 and size_of_chunk != partition_n:
            warn(
                f"Expected {partition_n} bytes, got {size_of_chunk} bytes at {self.pos}-{self.pos + size_of_chunk}."
            )
        return server_chunk
