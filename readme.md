<h1 align="center">RIO - A Remote BytesIO</h1>

<p align="center">Treat remote files like they exist in your memory.</p>

### Hey, what's this boring stuff?

RIO can be used to process contents **extremely** efficiently, without having to download the whole file:
- Downloading or streaming a file from an archive in the remote server.
- Processing a media file for its headers and metadata.
- Checking if an archive credential actually works.

And basically more.

### Prerequisites

- Remote file headers
    
    The server **must** accept ranges. This is shown by the `Accept-Ranges` header in the content response.

- Local parsing library

    The library that is used for parsing the file **must** support `io` objects as arguments. Using RIO with a library that sends the whole file to an external process or server will not be optimal. In those cases, you're better off downloading the full file.

### Example

```py
import zipfile

import humanize

from rio import RemoteIO

ZIP_FILE_URL = "https://oxygenos.oneplus.net/OnePlus8TOxygen_15.E.29_OTA_0290_all_2110091931_downgrade"

"""
HTTP/1.1 200 OK
...
Content-Type: application/java-archive
Content-Length: 2976128617 (3.0 GB)
...
Accept-Ranges: bytes
...
"""


rio = RemoteIO("GET", ZIP_FILE_URL)


with zipfile.ZipFile(rio) as zf:
    zf.extract("payload_properties.txt")

print(
    f"{rio.server_read / rio.server_full_size * 100:.010f}% == {humanize.naturalsize(rio.server_read)} out of {humanize.naturalsize(rio.server_full_size)}"
) # 0.0022027946% == 65.6 kB out of 3.0 GB
```