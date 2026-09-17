"""Async-first HTTP(S) client used for dataset downloads (standard library only).

Nothing here blocks the event loop: connections, TLS and body reads are all
native coroutines. A blocking ``urllib`` fallback is kept for environments
where the async path cannot work (exotic proxies), so ``detector db update``
never becomes unusable.
"""

from __future__ import annotations

import asyncio
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from ._version import __version__
from .exceptions import DownloadError

__all__ = [
    "USER_AGENT",
    "MAX_REDIRECTS",
    "fetch_bytes",
    "download",
    "download_sync",
    "ProgressFn",
]

USER_AGENT = f"detector/{__version__} (+https://github.com/scapking/detector-sdk)"
MAX_REDIRECTS = 6
ProgressFn = Optional[Callable[[str, int, Optional[int]], None]]

_REDIRECT_CODES = {301, 302, 303, 307, 308}


class _HttpResponse:
    __slots__ = ("headers", "reader", "status", "writer")

    def __init__(self, status: int, headers: Dict[str, str], reader: asyncio.StreamReader,
                 writer: asyncio.StreamWriter) -> None:
        self.status = status
        self.headers = headers
        self.reader = reader
        self.writer = writer

    async def close(self) -> None:
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:  # pragma: no cover - already closed
            pass


async def _read_headers(reader: asyncio.StreamReader, limit: int = 64 << 10) -> Tuple[int, Dict[str, str]]:
    head = await reader.readuntil(b"\r\n\r\n")
    if len(head) > limit:
        raise DownloadError("response headers too large")
    lines = head.decode("iso-8859-1").split("\r\n")
    parts = lines[0].split(" ", 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise DownloadError(f"malformed status line: {lines[0]!r}")
    status = int(parts[1])
    headers: Dict[str, str] = {}
    for line in lines[1:]:
        if ":" in line:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
    return status, headers


async def _open(url: str, *, timeout: float, headers: Dict[str, str]) -> _HttpResponse:
    from urllib.parse import urlsplit

    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise DownloadError(f"unsupported scheme: {url}")
    host = parts.hostname or ""
    port = parts.port or (443 if parts.scheme == "https" else 80)
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"

    ssl_context = ssl.create_default_context() if parts.scheme == "https" else None
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, ssl=ssl_context, server_hostname=host if ssl_context else None),
        timeout=timeout,
    )
    request_headers = {
        "Host": parts.netloc,
        "User-Agent": headers.get("User-Agent", USER_AGENT),
        "Accept": "*/*",
        "Accept-Encoding": "identity",
        "Connection": "close",
    }
    for key, value in headers.items():
        request_headers.setdefault(key, value)
    request = f"GET {path} HTTP/1.1\r\n" + "".join(
        f"{key}: {value}\r\n" for key, value in request_headers.items()
    ) + "\r\n"
    writer.write(request.encode("iso-8859-1"))
    await asyncio.wait_for(writer.drain(), timeout=timeout)
    status, response_headers = await asyncio.wait_for(_read_headers(reader), timeout=timeout)
    return _HttpResponse(status, response_headers, reader, writer)


async def _iter_body(response: _HttpResponse, chunk: int, timeout: float) -> "asyncio.AsyncIterator[bytes]":
    """Yield body bytes, transparently decoding chunked transfer encoding."""
    if response.headers.get("transfer-encoding", "").lower().strip() == "chunked":
        while True:
            size_line = await asyncio.wait_for(response.reader.readline(), timeout=timeout)
            size_text = size_line.split(b";")[0].strip()
            if not size_text:
                break
            try:
                size = int(size_text, 16)
            except ValueError as exc:
                raise DownloadError(f"bad chunk size: {size_line!r}") from exc
            if size == 0:
                while True:  # consume trailers
                    trailer = await asyncio.wait_for(response.reader.readline(), timeout=timeout)
                    if trailer in (b"\r\n", b"\n", b""):
                        break
                return
            remaining = size
            while remaining > 0:
                block = await asyncio.wait_for(response.reader.read(min(chunk, remaining)), timeout=timeout)
                if not block:
                    return
                remaining -= len(block)
                yield block
            await asyncio.wait_for(response.reader.read(2), timeout=timeout)  # trailing CRLF
        return
    while True:
        block = await asyncio.wait_for(response.reader.read(chunk), timeout=timeout)
        if not block:
            return
        yield block


async def fetch_bytes(
    url: str,
    *,
    timeout: float = 60,
    headers: Optional[Dict[str, str]] = None,
    max_redirects: int = MAX_REDIRECTS,
    max_bytes: Optional[int] = None,
) -> bytes:
    """GET a URL and return the body. Follows redirects; raises ``DownloadError``."""
    from urllib.parse import urljoin

    current = url
    for _ in range(max_redirects + 1):
        response = await _open(current, timeout=timeout, headers=dict(headers or {}))
        try:
            if response.status in _REDIRECT_CODES and response.headers.get("location"):
                current = urljoin(current, response.headers["location"])
                continue
            if response.status != 200:
                raise DownloadError(f"HTTP {response.status} for {current}", detail=current)
            buffer = bytearray()
            async for block in _iter_body(response, 1 << 20, timeout):
                buffer.extend(block)
                if max_bytes is not None and len(buffer) > max_bytes:
                    raise DownloadError(f"response larger than {max_bytes} bytes", detail=current)
            return bytes(buffer)
        finally:
            await response.close()
    raise DownloadError(f"too many redirects for {url}", detail=url)


async def download(
    url: str,
    dest: "str | os.PathLike",
    *,
    timeout: float = 300,
    chunk: int = 1 << 20,
    headers: Optional[Dict[str, str]] = None,
    progress: ProgressFn = None,
    name: Optional[str] = None,
    max_redirects: int = MAX_REDIRECTS,
    retries: int = 1,
) -> int:
    """Stream a URL into ``dest`` atomically. Returns the number of bytes written."""
    from urllib.parse import urljoin

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    label = name or dest.name
    last_error: Optional[Exception] = None

    for attempt in range(retries + 1):
        tmp = dest.with_name(dest.name + f".part{os.getpid()}.{attempt}")
        current = url
        try:
            for _ in range(max_redirects + 1):
                response = await _open(current, timeout=timeout, headers=dict(headers or {}))
                try:
                    if response.status in _REDIRECT_CODES and response.headers.get("location"):
                        current = urljoin(current, response.headers["location"])
                        continue
                    if response.status != 200:
                        raise DownloadError(f"HTTP {response.status} for {current}", detail=current)
                    total_header = response.headers.get("content-length")
                    total = int(total_header) if total_header and total_header.isdigit() else None
                    written = 0
                    with open(tmp, "wb") as handle:
                        async for block in _iter_body(response, chunk, timeout):
                            handle.write(block)
                            written += len(block)
                            if progress:
                                progress(label, written, total)
                    if total is not None and written != total:
                        raise DownloadError(f"incomplete download: {written}/{total} bytes", detail=current)
                    os.replace(tmp, dest)
                    return written
                finally:
                    await response.close()
            raise DownloadError(f"too many redirects for {url}", detail=url)
        except (DownloadError, OSError, asyncio.TimeoutError, ssl.SSLError) as exc:
            tmp.unlink(missing_ok=True)
            last_error = exc
            if attempt >= retries:
                break
            await asyncio.sleep(1.5 * (attempt + 1))
    raise DownloadError(f"download failed: {last_error}", detail=url)


def download_sync(
    url: str,
    dest: "str | os.PathLike",
    *,
    timeout: float = 300,
    chunk: int = 1 << 20,
    headers: Optional[Dict[str, str]] = None,
    progress: ProgressFn = None,
    name: Optional[str] = None,
) -> int:
    """Blocking fallback (``urllib``) with the same contract as :func:`download`."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    label = name or dest.name
    tmp = dest.with_name(dest.name + f".part{os.getpid()}")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            total_header = response.headers.get("Content-Length")
            total = int(total_header) if total_header and total_header.isdigit() else None
            written = 0
            with open(tmp, "wb") as handle:
                while True:
                    block = response.read(chunk)
                    if not block:
                        break
                    handle.write(block)
                    written += len(block)
                    if progress:
                        progress(label, written, total)
        if total is not None and written != total:
            raise DownloadError(f"incomplete download: {written}/{total} bytes", detail=url)
        os.replace(tmp, dest)
        return written
    except (urllib.error.URLError, OSError, DownloadError) as exc:
        tmp.unlink(missing_ok=True)
        raise DownloadError(f"download failed: {exc}", detail=url) from exc
