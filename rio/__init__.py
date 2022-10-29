import io
from warnings import warn

import requests

from .utils import ExistingRanges


class RemoteIO(io.IOBase):
    def __init__(self, *args, session=None, **kwargs):

        self.session: requests.Session = session or requests.Session()
        self.request_args = args

        self.headers = kwargs.pop("headers", {})
        self.request_kwargs = kwargs

        self.pos = 0

        self.buffer = io.BytesIO()
        self.existing_ranges = ExistingRanges()

        self.streaming_response = None

        self.server_read = 0
        self.server_full_size = None

    def seek(self, offset, whence=0):
        """
        Update the position of the stream to offset bytes,
        relative to the position indicated by whence.

        Check if the byte in the offset is already in the buffer,
        if not, open a stream from that offset.

        Initial seek would not know the size of the file, so
        seeking relative to EOF (whence = 2) will not be able
        to give the position.

        After a response, the size of the file will be known and
        the position will be approximated.

        The request will not be made according to the read, hence
        previous stream will be closed if a new position is seeked at.
        """
        assert whence in (0, 1, 2), "Invalid whence"

        if whence == 0:
            self.pos = offset
            hypothetical_range = (self.pos, self.pos + 1)
        else:
            if whence == 1:
                self.pos += offset
                offset = self.pos
                hypothetical_range = (self.pos, self.pos + 1)
            else:
                if whence == 2:
                    offset = -abs(offset)
                    hypothetical_range = None

        total_size = self.attempt_size_resolving()

        if hypothetical_range is not None:
            if hypothetical_range in self.existing_ranges:
                return
        else:
            if total_size is not None:
                if (
                    total_size + offset,
                    total_size + offset + 1,
                ) in self.existing_ranges:
                    return

        if offset >= 0:
            byte_range = f"{offset}-"
        else:
            byte_range = f"-{abs(offset)}"

        if self.streaming_response is not None:
            self.streaming_response.close()

        self.streaming_response = self.session.request(
            *self.request_args,
            headers=self.headers | {"Range": f"bytes={byte_range}"},
            **self.request_kwargs,
            stream=True,
        )
        setattr(self.streaming_response, "offset", offset)

        if whence == 2:
            self.pos = self.attempt_size_resolving() + offset

    def read(self, n=None):
        """
        Read n bytes from the stream, if n is None,
        read all the stream.

        The stream will be cached in a buffer so same
        positional reads will not require any stream reading.
        """

        if self.pos == 0 and n is None:
            warn(
                "The RIO object is being read in full which is the worst case for its usage. "
                "Using packages that don't try to read the entire file is highly recommended."
            )

        partitions = self.existing_ranges.iter_partition(
            self.pos,
            (self.pos + n) if n is not None else self.attempt_size_resolving(),
        )

        chunk = b""

        for start, end in partitions:
            self.buffer.seek(start)
            partition_n = end - start

            if (start, end - 1) in self.existing_ranges:
                chunk += self.buffer.read(partition_n)
            else:
                self.seek(start)
                server_chunk = self.streaming_response.raw.read(partition_n)

                self.server_read += self.buffer.write(server_chunk)
                self.existing_ranges.add(start, end)

                chunk += server_chunk

        self.pos += len(chunk)

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

        self.session.close()

    def attempt_size_resolving(self):
        """
        Approximate the size of the file by the streaming responses
        opened through the .seek method.

        Content-Range header is expected to be in the form of
        "bytes 0-100/1000" where the last number is the size of the file.

        Content-Length header is not typically common but kept as a failsafe
        for servers that are not conventional.
        """
        if self.server_full_size is not None:
            return self.server_full_size

        if self.streaming_response is None:
            return

        headers = self.streaming_response.headers

        if "Content-Range" in headers:
            full_size = int(headers["Content-Range"].split("/")[1])
        else:
            if "Content-Length" in headers:
                full_size = (
                    int(headers["Content-Length"]) + self.streaming_response.offset
                )

        self.server_full_size = full_size

        return full_size
