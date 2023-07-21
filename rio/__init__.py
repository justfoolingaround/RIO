import io
import logging
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
        buffer: t.Optional[t.BinaryIO] = None,
        **kwargs,
    ):
        self.session: requests.Session = session or requests.Session()
        self.request_args = args

        self.headers: t.Dict[str, str] = kwargs.pop("headers", {})
        self.request_kwargs = kwargs

        self.pos = 0

        self.buffer = buffer or io.BytesIO()
        self.existing_ranges = ExistingRanges()

        self.streaming_response: t.Optional[requests.Response] = None

        self.server_read: int = 0
        self.server_full_size: t.Optional[int] = None

        self.logger = logging.getLogger("remoteio")

    def seek(self, offset: int, whence: WHENCE_TYPE = 0):
        assert whence in (0, 1, 2), "Invalid whence"

        initial = self.pos

        if whence == 0:
            if self.pos == offset:
                self.logger.info(
                    f"Already at position {self.pos}; not attempting to seek at all."
                )
                return

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

                    total_size = self.attempt_size_resolving()

                    if total_size is None:
                        raise ValueError(
                            "Cannot seek from EOF when the size is unknown"
                        )
                    self.pos = total_size + offset
                    hypothetical_range = (total_size + offset, total_size)
                else:
                    raise ValueError("Invalid whence, must be 0, 1 or 2")

        if initial == self.pos:
            self.logger.info(
                f"Already at position {self.pos}; not opening an unnecessary stream."
            )
            return

        if hypothetical_range in self.existing_ranges:
            self.logger.info(
                f"Expected data at the range {hypothetical_range} is already in the buffer; not opening an unnecessary stream."
            )
            return

        if whence <= 1:
            byte_range = f"{offset}-"
        else:
            byte_range = f"-{abs(offset)}"

        if self.streaming_response is not None:
            self.logger.info(
                f"Closing the previous stream at position: {initial}, starting anew at position: {self.pos}"
            )
            self.streaming_response.close()

        self.logger.info(f"Requesting byte range: {byte_range} from the server.")
        self.streaming_response = self.session.request(
            *self.request_args,
            headers=self.headers | {"Range": f"bytes={byte_range}"},
            **self.request_kwargs,
            stream=True,
        )

    def read(self, n: int = -1):
        if self.pos == 0 and not n + 1:
            warn(
                "The RIO object is being read in full which is the worst case for its usage. "
                "Using packages that don't try to read the entire file is highly recommended."
            )

        file_size = self.attempt_size_resolving()

        partitions = (
            self.existing_ranges.iter_partition(
                self.pos,
                (self.pos + n) if n + 1 else file_size,
            )
            if file_size is not None
            else ((self.pos, (self.pos + n) if n + 1 else None),)
        )

        chunk = b""

        for start, end in partitions:
            self.buffer.seek(start)
            partition_n = (end - start) if end is not None else -1

            if (start, end) in self.existing_ranges:
                self.logger.info(
                    f"Reading data at the range {start}-{end} from existing buffer."
                )
                chunk += self.buffer.read(partition_n)

            else:
                self.seek(start)

                if self.streaming_response is None:
                    raise RuntimeError("The stream was not open after seeking")

                server_chunk: bytes = self.streaming_response.raw.read(partition_n)

                size_of_chunk = len(server_chunk)

                if partition_n != -1 and size_of_chunk != partition_n:
                    warn(
                        f"Expected {partition_n} bytes, got {size_of_chunk} bytes at {start}-{end if end is not None else ''}."
                    )

                self.server_read += self.buffer.write(server_chunk)
                self.existing_ranges.add(start, start + len(server_chunk))

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

        try:
            self.buffer.close()
        except AttributeError:
            pass

        self.session.close()

    def attempt_size_resolving(self):
        if self.server_full_size is not None:
            return self.server_full_size

        if self.streaming_response is not None:
            headers = self.streaming_response.headers

            if "Content-Range" in headers:
                self.server_full_size = int(headers["Content-Range"].split("/")[1])

                self.logger.info(
                    f"Size of the file was resolved via an existing stream: {self.server_full_size}"
                )

                return self.server_full_size

        kwargs = self.request_kwargs.copy()

        if kwargs.pop("method", None) is None:
            args = self.request_args[1:]
        else:
            args = self.request_args

        response_headers = self.session.head(
            *args, **kwargs, headers=self.headers, allow_redirects=True
        ).headers

        content_length = int(response_headers.get("Content-Length", 0))

        if content_length:
            self.server_full_size = content_length
            self.logger.info(
                f"Size of the file was resolved via a HEAD request: {self.server_full_size}"
            )

            return self.server_full_size

        headers = self.headers.copy()
        headers["Range"] = "bytes=0-"

        nc_response = self.session.get(*args, **kwargs, headers=headers, stream=True)

        if "Content-Range" in nc_response.headers:
            self.server_full_size = int(
                nc_response.headers["Content-Range"].split("/")[1]
            )

            self.logger.info(
                f"Size of the file was resolved via a non-conventional streaming method: {self.server_full_size}"
            )
            nc_response.close()
            return self.server_full_size

        return None
