import io
from warnings import warn

import requests


class ExistingRanges:
    def __init__(self):
        self.existing_ranges = set()

    def add(self, start, end):
        if (start, end) in self:
            return

        for starting, ending in list(self.existing_ranges):

            if ending == start:
                start = starting
                self.existing_ranges.remove((starting, ending))
            else:
                if starting == end:
                    end = ending
                    self.existing_ranges.remove((starting, ending))

        self.existing_ranges.add((start, end))

    def __contains__(self, other):
        (start, end) = other
        return any(
            starting <= start < ending and starting <= end < ending
            for starting, ending in self.existing_ranges
        )


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

        if self.pos == 0 and n is None:
            warn(
                "The RIO object is being read in full which is the worst case for its usage. Using packages that don't try to read the entire file is highly recommended."
            )

        if (
            self.pos,
            (self.pos + n - 1) if n is not None else self.attempt_size_resolving(),
        ) in self.existing_ranges:
            self.buffer.seek(self.pos)

            chunk = self.buffer.read(n)
            chunk_size = len(chunk)

            self.pos += chunk_size

            if (
                chunk_size < n & self.buffer.tell() == self.attempt_size_resolving()
            ) | chunk_size == n:
                return chunk

            self.seek(n + chunk_size, 1)
            return self.read(chunk_size - n)

        if self.streaming_response is None:
            self.seek(self.pos)

        chunk = self.streaming_response.raw.read(n)
        chunk_size = len(chunk)
        self.server_read += chunk_size

        self.buffer.seek(self.pos, 0)
        self.buffer.write(chunk)
        self.existing_ranges.add(self.pos, self.pos + chunk_size)

        self.pos += chunk_size

        return chunk

    def tell(self):
        return self.pos

    def attempt_size_resolving(self):

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
