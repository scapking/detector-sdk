# ruff: noqa
"""Lazy block-based reader for MaxMind DB v2.0 files (no full extraction).

Bundled databases ship as ``.bz`` containers: the raw MMDB is cut into fixed-size
blocks, each compressed independently with zstd, followed by an index. A lookup
decompresses only the blocks the binary-search traversal actually touches - a
few hundred KB instead of the whole 100-250 MB - so first use costs milliseconds
and never materialises the full database on disk. The OS page cache keeps
repeated queries fast.

The decode logic below is the official pure-Python MaxMind reader (MIT licence,
Copyright (c) 2013-2024 MaxMind, Inc. and contributors, see NOTICE): it reads
through a tiny ``LazyBuffer`` exposing the same ``[]`` / ``rfind`` interface the
reference decoder needs, so nothing in the format handling was reimplemented.
"""

from __future__ import annotations

import contextlib
import ipaddress
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import IO, TYPE_CHECKING, Any, AnyStr, Optional

MODE_AUTO = 0
MODE_MMAP = 1
MODE_FILE = 2
MODE_MEMORY = 3
MODE_FD = 4


class InvalidDatabaseError(RuntimeError):
    """Raised when the database is corrupt or not a MaxMind DB file."""


if TYPE_CHECKING:

    Record = Any

_IPV4_MAX_NUM = 2**32


"""Decoder for the MaxMind DB data section."""


import struct
from typing import TYPE_CHECKING

# Per-lookup value limit recommended by the MaxMind DB specification. It stops
# pointer fan-out, where nested containers share targets that would otherwise
# cost 2**depth decode operations. The root costs one value. Arrays charge each
# element, maps charge each key and value, and pointers cost no extra value.
# Real records decode a few hundred values, leaving a wide margin.
# An explicit depth limit catches container cycles and overly nested data.
# Python's recursion limit may fire first, which decode converts to the same
# error. The explicit limit also applies when callers raise Python's limit.
_MAX_VALUES = 1 << 16
_MAX_DEPTH = 512
# Per-lookup limit on the total string and bytes payload materialized, matching
# libmaxminddb and the Go reader. It stops a payload amplification, where many
# pointers to one large value would otherwise materialize N * size bytes from a
# small file. Each string or bytes value is charged its length wherever it is
# decoded, so re-decoding a shared target through another pointer recharges.
_MAX_PAYLOAD_BYTES = 1 << 21
# The widest fixed-width integer the format defines is the 16-byte uint128; a
# declared size past that is malformed and could copy attacker-controlled bytes.
_MAX_UINT_BYTES = 16
_MAX_INT32_BYTES = 4
# Added to a pointer value, by pointer size. A 4-byte pointer adds nothing.
_POINTER_VALUE_OFFSETS = (0, 0, 2048, 526336)
_TOO_MANY_VALUES = (
    "The MaxMind DB file's data section exceeds the maximum number of values"
)
_TOO_DEEP = "The MaxMind DB file's data section exceeds the maximum depth"
_TOO_LARGE = "The MaxMind DB file's data section exceeds the maximum payload size"
_BAD_DATA = (
    "The MaxMind DB file's data section contains bad data "
    "(unknown data type or corrupt data)"
)


@dataclass
class _DecodeBudget:
    """Shared counters for one record or metadata decode."""

    values_left: int
    depth: int
    payload_left: int


class Decoder:
    """Decoder for the data section of the MaxMind DB."""

    def __init__(
        self,
        database_buffer: FileBuffer | mmap.mmap | bytes,
        pointer_base: int = 0,
        pointer_test: bool = False,
    ) -> None:
        """Create a Decoder for a MaxMind DB.

        Arguments:
            database_buffer: an mmap'd MaxMind DB file.
            pointer_base: the base number to use when decoding a pointer
            pointer_test: used for internal unit testing of pointer code

        """
        self._pointer_test = pointer_test
        self._buffer = database_buffer
        self._pointer_base = pointer_base

    def _decode_array(
        self,
        size: int,
        offset: int,
        budget: _DecodeBudget,
    ) -> tuple[list[Record], int]:
        remaining = budget.values_left - size
        if remaining < 0:
            raise InvalidDatabaseError(_TOO_MANY_VALUES)
        budget.values_left = remaining
        depth = budget.depth + 1
        if depth > _MAX_DEPTH:
            raise InvalidDatabaseError(_TOO_DEEP)
        budget.depth = depth
        array = []
        decode = self._decode
        for _ in range(size):
            (value, offset) = decode(offset, budget, False)
            array.append(value)
        budget.depth -= 1
        return array, offset

    def _decode_boolean(
        self,
        size: int,
        offset: int,
        _budget: _DecodeBudget,
    ) -> tuple[bool, int]:
        return size != 0, offset

    def _decode_bytes(
        self,
        size: int,
        offset: int,
        budget: _DecodeBudget,
    ) -> tuple[bytes, int]:
        # Charge the payload before copying so a crafted size cannot force a
        # large allocation, and so pointers reusing one target recharge.
        remaining = budget.payload_left - size
        if remaining < 0:
            raise InvalidDatabaseError(_TOO_LARGE)
        budget.payload_left = remaining
        new_offset = offset + size
        return self._buffer[offset:new_offset], new_offset

    def _decode_double(
        self,
        size: int,
        offset: int,
        _budget: _DecodeBudget,
    ) -> tuple[float, int]:
        self._verify_size(size, 8)
        new_offset = offset + size
        packed_bytes = self._buffer[offset:new_offset]
        (value,) = struct.unpack(b"!d", packed_bytes)
        return value, new_offset

    def _decode_float(
        self,
        size: int,
        offset: int,
        _budget: _DecodeBudget,
    ) -> tuple[float, int]:
        self._verify_size(size, 4)
        new_offset = offset + size
        packed_bytes = self._buffer[offset:new_offset]
        (value,) = struct.unpack(b"!f", packed_bytes)
        return value, new_offset

    def _decode_int32(
        self,
        size: int,
        offset: int,
        _budget: _DecodeBudget,
    ) -> tuple[int, int]:
        if size > _MAX_INT32_BYTES:
            raise InvalidDatabaseError(_BAD_DATA)
        if size == 0:
            return 0, offset
        new_offset = offset + size
        packed_bytes = self._buffer[offset:new_offset]

        if size != 4:
            packed_bytes = packed_bytes.rjust(4, b"\x00")
        (value,) = struct.unpack(b"!i", packed_bytes)
        return value, new_offset

    def _decode_map(
        self,
        size: int,
        offset: int,
        budget: _DecodeBudget,
    ) -> tuple[dict[str, Record], int]:
        # A map entry decodes a key and a value, so it costs two values.
        remaining = budget.values_left - size * 2
        if remaining < 0:
            raise InvalidDatabaseError(_TOO_MANY_VALUES)
        budget.values_left = remaining
        depth = budget.depth + 1
        if depth > _MAX_DEPTH:
            raise InvalidDatabaseError(_TOO_DEEP)
        budget.depth = depth
        container: dict[str, Record] = {}
        decode = self._decode
        for _ in range(size):
            (key, offset) = decode(offset, budget, False)
            (value, offset) = decode(offset, budget, False)
            container[key] = value  # type: ignore[index]
        budget.depth -= 1
        return container, offset

    def _decode_pointer(
        self,
        size: int,
        offset: int,
        budget: _DecodeBudget,
    ) -> tuple[Any, int]:
        pointer_size = (size >> 3) + 1
        new_offset = offset + pointer_size
        pointer_bytes = self._buffer[offset:new_offset]
        if len(pointer_bytes) != pointer_size:
            raise InvalidDatabaseError(_BAD_DATA)
        pointer = int.from_bytes(pointer_bytes, "big")
        if pointer_size < 4:
            # The low three bits of the ctrl byte are the high bits of the
            # pointer, and sizes 2 and 3 add a fixed offset.
            pointer |= (size & 0x7) << (pointer_size << 3)
            pointer += _POINTER_VALUE_OFFSETS[pointer_size]
        pointer += self._pointer_base

        if self._pointer_test:
            return pointer, new_offset

        # The value at the pointer's position was charged by its containing
        # array or map, so the target costs nothing more. Only the depth changes.
        depth = budget.depth + 1
        if depth > _MAX_DEPTH:
            raise InvalidDatabaseError(_TOO_DEEP)
        budget.depth = depth
        (value, _) = self._decode(pointer, budget, True)
        budget.depth -= 1
        return value, new_offset

    def _decode_uint(
        self,
        size: int,
        offset: int,
        _budget: _DecodeBudget,
    ) -> tuple[int, int]:
        # Reject a declared size past the widest defined unsigned integer before
        # copying, so a crafted size cannot force a large allocation.
        if size > _MAX_UINT_BYTES:
            raise InvalidDatabaseError(_BAD_DATA)
        new_offset = offset + size
        uint_bytes = self._buffer[offset:new_offset]
        return int.from_bytes(uint_bytes, "big"), new_offset

    def decode(self, offset: int) -> tuple[Any, int]:
        """Decode a section of the data section starting at offset.

        Arguments:
            offset: the location of the data structure to decode

        """
        # Each call gets its own budget, shared by recursive calls. Charge the
        # root here.
        try:
            return self._decode(
                offset,
                _DecodeBudget(_MAX_VALUES - 1, 0, _MAX_PAYLOAD_BYTES),
                False,
            )
        except RecursionError as ex:
            raise InvalidDatabaseError(_TOO_DEEP) from ex
        except (IndexError, struct.error) as ex:
            # Convert failed buffer indexing and fixed-width unpacking.
            raise InvalidDatabaseError(_BAD_DATA) from ex

    # Keep type dispatch inline to avoid another call for every decoded value.
    # The positional booleans are intentional: keywords and omitted defaults
    # prevented CPython from using its fastest call path in our benchmarks.
    # pointer_target rejects pointers to other pointers.
    def _decode(
        self,
        offset: int,
        budget: _DecodeBudget,
        pointer_target: bool,
    ) -> tuple[Any, int]:
        new_offset = offset + 1
        ctrl_byte = self._buffer[offset]
        type_num = ctrl_byte >> 5
        # Extended type
        if not type_num:
            (type_num, new_offset) = self._read_extended(new_offset)

        size = ctrl_byte & 0x1F
        # Sizes under 29 are stored in the ctrl byte, and a pointer's size bits
        # are not a size. Skip the call for that common case.
        if size >= 29 and type_num != 1:
            (size, new_offset) = self._size_from_ctrl_byte(size, new_offset)
        # Put common types first to reduce comparisons during real lookups.
        match type_num:
            case 2:
                # Strings are most of the values in a real database. Decode them
                # here to save a method call.
                # Charge the payload before copying so a crafted size cannot force
                # a large allocation, and so pointers reusing one target recharge.
                remaining = budget.payload_left - size
                if remaining < 0:
                    raise InvalidDatabaseError(_TOO_LARGE)
                budget.payload_left = remaining
                end = new_offset + size
                return self._buffer[new_offset:end].decode("utf-8"), end
            case 1:
                if pointer_target:
                    raise InvalidDatabaseError(_BAD_DATA)
                return self._decode_pointer(size, new_offset, budget)
            case 7:
                return self._decode_map(size, new_offset, budget)
            case 6 | 5 | 9 | 10:  # uint32, uint16, uint64, uint128
                return self._decode_uint(size, new_offset, budget)
            case 11:
                return self._decode_array(size, new_offset, budget)
            case 3:
                return self._decode_double(size, new_offset, budget)
            case 4:
                return self._decode_bytes(size, new_offset, budget)
            case 8:
                return self._decode_int32(size, new_offset, budget)
            case 14:
                return self._decode_boolean(size, new_offset, budget)
            case 15:
                return self._decode_float(size, new_offset, budget)
            case _:
                msg = f"Unexpected type number ({type_num}) encountered"
                raise InvalidDatabaseError(msg)

    def _read_extended(self, offset: int) -> tuple[int, int]:
        next_byte = self._buffer[offset]
        type_num = next_byte + 7
        if type_num < 7:
            msg = (
                "Something went horribly wrong in the decoder. An "
                f"extended type resolved to a type number < 8 ({type_num})"
            )
            raise InvalidDatabaseError(
                msg,
            )
        return type_num, offset + 1

    @staticmethod
    def _verify_size(expected: int, actual: int) -> None:
        if expected != actual:
            raise InvalidDatabaseError(_BAD_DATA)

    def _size_from_ctrl_byte(self, size: int, offset: int) -> tuple[int, int]:
        # Called only for size codes 29 to 31, which are followed by size bytes.
        if size == 29:
            size = 29 + self._buffer[offset]
            return size, offset + 1

        # Using unpack rather than int_from_bytes as it is faster
        # here and below.
        if size == 30:
            new_offset = offset + 2
            size_bytes = self._buffer[offset:new_offset]
            size = 285 + struct.unpack(b"!H", size_bytes)[0]
            return size, new_offset

        new_offset = offset + 3
        size_bytes = self._buffer[offset:new_offset]
        size = struct.unpack(b"!I", b"\x00" + size_bytes)[0] + 65821
        return size, new_offset


"""Pure-Python reader for the MaxMind DB file format."""





if TYPE_CHECKING:
    from collections.abc import Iterator
    from os import PathLike

    from maxminddb.types import Record
    from typing_extensions import Self

_IPV4_MAX_NUM = 2**32


class Reader:
    """A pure Python implementation of a reader for the MaxMind DB format.

    IP addresses can be looked up using the ``get`` method.
    """

    _DATA_SECTION_SEPARATOR_SIZE = 16
    _METADATA_START_MARKER = b"\xab\xcd\xefMaxMind.com"

    _buffer: bytes | FileBuffer | "mmap.mmap"
    _buffer_size: int
    closed: bool
    _decoder: Decoder
    _metadata: Metadata
    _record_size: int
    _ipv4_start: int

    def __init__(
        self,
        database: AnyStr | int | PathLike | IO,
        mode: int = MODE_AUTO,
    ) -> None:
        """Reader for the MaxMind DB file format.

        Arguments:
            database: A path to a valid MaxMind DB file such as a GeoIP database
                      file, or a file descriptor in the case of MODE_FD.
            mode: mode to open the database with. Valid mode are:
                  * MODE_MMAP - read from memory map.
                  * MODE_FILE - read database as standard file.
                  * MODE_MEMORY - load database into memory.
                  * MODE_AUTO - tries MODE_MMAP and then MODE_FILE. Default.
                  * MODE_FD - the param passed via database is a file descriptor, not
                              a path. This mode implies MODE_MEMORY.

        """
        filename = self._load_buffer(database, mode)

        # Include validation errors in this cleanup scope. TRY301 is suppressed
        # because the handler only closes the buffer and re-raises the error.
        try:
            metadata_start = self._buffer.rfind(
                self._METADATA_START_MARKER,
                max(0, self._buffer_size - 128 * 1024),
            )

            if metadata_start == -1:
                msg = (
                    f"Error opening database file ({filename}). "
                    "Is this a valid MaxMind DB file?"
                )
                raise InvalidDatabaseError(
                    msg,
                )

            metadata_start += len(self._METADATA_START_MARKER)
            metadata_decoder = Decoder(self._buffer, metadata_start)
            (metadata, _) = metadata_decoder.decode(metadata_start)

            if not isinstance(metadata, dict):
                msg = f"Error reading metadata in database file ({filename})."
                raise InvalidDatabaseError(
                    msg,
                )

            self._metadata = Metadata(**metadata)
            self._record_size = self._metadata.record_size
            if self._record_size not in (24, 28, 32):
                msg = f"Unknown record size: {self._record_size}"
                raise InvalidDatabaseError(msg)
            if self._metadata.node_count < 0:
                msg = f"Invalid node count: {self._metadata.node_count}"
                raise InvalidDatabaseError(msg)

            # Traversal reads nodes below node_count. Once the tree fits, those
            # reads need no length checks of their own.
            tree_end = (
                self._metadata.search_tree_size + self._DATA_SECTION_SEPARATOR_SIZE
            )
            if tree_end > self._buffer_size:
                msg = (
                    f"Error opening database file ({filename}). The search tree "
                    "extends past the end of the file."
                )
                raise InvalidDatabaseError(msg)

            self._decoder = Decoder(
                self._buffer,
                self._metadata.search_tree_size + self._DATA_SECTION_SEPARATOR_SIZE,
            )
            self.closed = False
            self._ipv4_start = None  # computed lazily in _start_node, keeps open() fast
        except BaseException:
            # Release the buffer on any initialization failure.
            self.close()
            raise

    def metadata(self) -> Metadata:
        """Return the metadata associated with the MaxMind DB file."""
        return self._metadata

    def get(self, ip_address: str | IPv6Address | IPv4Address) -> Record | None:
        """Return the record for the ip_address in the MaxMind DB.

        Arguments:
            ip_address: an IP address in the standard string notation

        """
        (record, _) = self.get_with_prefix_len(ip_address)
        return record

    def get_with_prefix_len(
        self,
        ip_address: str | IPv6Address | IPv4Address,
    ) -> tuple[Record | None, int]:
        """Return a tuple with the record and the associated prefix length.

        Arguments:
            ip_address: an IP address in the standard string notation

        """
        if isinstance(ip_address, str):
            address = ipaddress.ip_address(ip_address)
        else:
            address = ip_address

        try:
            packed_address = bytearray(address.packed)
        except AttributeError as ex:
            msg = "argument 1 must be a string or ipaddress object"
            raise TypeError(msg) from ex

        if address.version == 6 and self._metadata.ip_version == 4:
            msg = (
                f"Error looking up {ip_address}. You attempted to look up "
                "an IPv6 address in an IPv4-only database."
            )
            raise ValueError(
                msg,
            )

        (pointer, prefix_len) = self._find_address_in_tree(packed_address)

        if pointer:
            return self._resolve_data_pointer(pointer), prefix_len
        return None, prefix_len

    def __iter__(self) -> Iterator:
        return self._generate_children(0, 0, 0)

    def _generate_children(self, node: int, depth: int, ip_acc: int) -> Iterator:
        if ip_acc != 0 and node == self._ipv4_start:
            # Skip nodes aliased to IPv4
            return

        node_count = self._metadata.node_count
        if node > node_count:
            bits = 128 if self._metadata.ip_version == 6 else 32
            ip_acc <<= bits - depth
            if ip_acc <= _IPV4_MAX_NUM and bits == 128:
                depth -= 96
            yield (
                ipaddress.ip_network((ip_acc, depth)),
                self._resolve_data_pointer(
                    node,
                ),
            )
        elif node < node_count:
            left = self._read_node(node, 0)
            ip_acc <<= 1
            depth += 1
            yield from self._generate_children(left, depth, ip_acc)
            right = self._read_node(node, 1)
            yield from self._generate_children(right, depth, ip_acc | 1)

    def _find_address_in_tree(self, packed: bytearray) -> tuple[int, int]:
        bit_count = len(packed) * 8
        node = self._start_node(bit_count)
        node_count = self._metadata.node_count

        i = 0
        while i < bit_count and node < node_count:
            bit = 1 & (packed[i >> 3] >> 7 - (i % 8))
            node = self._read_node(node, bit)
            i = i + 1

        if node == node_count:
            # Record is empty
            return 0, i
        if node > node_count:
            return node, i

        msg = "Invalid node in search tree"
        raise InvalidDatabaseError(msg)

    def _start_node(self, length: int) -> int:
        if self._metadata.ip_version == 6 and length == 32:
            if self._ipv4_start is None:
                # The IPv4 start node is found by descending 96 zero-bits from the
                # root. Cheap (one call), so it is deferred out of open() to keep
                # first-use latency low.
                node = 0
                for _ in range(96):
                    if node >= self._metadata.node_count:
                        break
                    node = self._read_node(node, 0)
                self._ipv4_start = node
            return self._ipv4_start
        return 0

    def _read_node(self, node_number: int, index: int) -> int:
        record_size = self._record_size
        if record_size == 28:
            # Two 28-bit records share the middle byte: its high nibble
            # belongs to the left record and its low nibble to the right.
            base_offset = node_number * 7
            if index:
                offset = base_offset + 3
                record = int.from_bytes(self._buffer[offset : offset + 4], "big")
                return record & 0x0FFFFFFF
            record = int.from_bytes(self._buffer[base_offset : base_offset + 4], "big")
            return (record >> 8) | ((record & 0xF0) << 20)
        if record_size == 24:
            offset = node_number * 6 + index * 3
            return int.from_bytes(self._buffer[offset : offset + 3], "big")
        if record_size == 32:
            offset = node_number * 8 + index * 4
            return int.from_bytes(self._buffer[offset : offset + 4], "big")
        msg = f"Unknown record size: {record_size}"
        raise InvalidDatabaseError(msg)

    def _resolve_data_pointer(self, pointer: int) -> Record:
        resolved = pointer - self._metadata.node_count + self._metadata.search_tree_size

        if resolved >= self._buffer_size:
            msg = "The MaxMind DB file's search tree is corrupt"
            raise InvalidDatabaseError(msg)

        (data, _) = self._decoder.decode(resolved)
        return data

    def _load_buffer(
        self, database: AnyStr | int | PathLike | IO, mode: int = MODE_AUTO
    ) -> str:
        filename: Any
        if (mode == MODE_AUTO and mmap) or mode == MODE_MMAP:
            with open(database, "rb") as db_file:  # type: ignore[arg-type]
                self._buffer = mmap.mmap(db_file.fileno(), 0, access=mmap.ACCESS_READ)
                self._buffer_size = self._buffer.size()
            filename = database
        elif mode in (MODE_AUTO, MODE_FILE):
            raise ValueError("MODE_FILE is not supported by the lazy reader")
            self._buffer_size = self._buffer.size()
            filename = database
        elif mode == MODE_MEMORY:
            with open(database, "rb") as db_file:  # type: ignore[arg-type]
                buf = db_file.read()
                self._buffer = buf
                self._buffer_size = len(buf)
            filename = database
        elif mode == MODE_FD:
            self._buffer = database.read()  # type: ignore[union-attr]
            self._buffer_size = len(self._buffer)  # type: ignore[arg-type]
            # io buffers are not guaranteed to have a name attribute
            if hasattr(database, "name"):
                filename = database.name  # type: ignore[union-attr]
            else:
                filename = f"<{type(database)}>"
        else:
            msg = (
                f"Unsupported open mode ({mode}). Only MODE_AUTO, MODE_FILE, "
                "MODE_MEMORY and MODE_FD are supported by the pure Python "
                "Reader"
            )
            raise ValueError(
                msg,
            )

        return filename

    def close(self) -> None:
        """Close the MaxMind DB file and returns the resources to the system.

        Calling this method while reads are in progress may cause exceptions.
        """
        with contextlib.suppress(AttributeError):
            self._buffer.close()  # type: ignore[union-attr]

        self.closed = True

    def __exit__(self, *_) -> None:
        self.close()

    def __enter__(self) -> Self:
        if self.closed:
            msg = "Attempt to reopen a closed MaxMind DB"
            raise ValueError(msg)
        return self


@dataclass(kw_only=True, frozen=True)
class Metadata:
    """Metadata for the MaxMind DB reader."""

    binary_format_major_version: int
    """
    The major version number of the binary format used when creating the
    database.
    """

    binary_format_minor_version: int
    """
    The minor version number of the binary format used when creating the
    database.
    """

    build_epoch: int
    """The Unix epoch for the build time of the database."""

    database_type: str
    """A string identifying the database type, e.g., "GeoIP2-City"."""

    description: dict[str, str]
    """A map from locales to text descriptions of the database."""

    ip_version: int
    """
    The IP version of the data in a database. A value of "4" means the
    database only supports IPv4. A database with a value of "6" may support
    both IPv4 and IPv6 lookups.
    """

    languages: list[str]
    """A list of locale codes supported by the database."""

    node_count: int
    """The number of nodes in the database."""

    record_size: int
    """The bit size of a record in the search tree."""

    @property
    def node_byte_size(self) -> int:
        """The size of a node in bytes."""
        return self.record_size // 4

    @property
    def search_tree_size(self) -> int:
        """The size of the search tree."""
        return self.node_count * self.node_byte_size


# --------------------------------------------------------------------------- #
# Lazy block buffer (`.bz` containers shipped with the SDK)
# --------------------------------------------------------------------------- #

import struct as _struct
import threading as _threading
from collections import OrderedDict

import zstandard as _zstd

_MAGIC = b"DETBLK01"
_HEAD_FMT = _struct.Struct(">8sIII")   # magic, block_size, block_count, raw_size
_INDEX_FMT = _struct.Struct(">I")      # per-block compressed size


class LazyBuffer:
    """Byte-addressed view of a ``.bz`` block container.

    Implements exactly the interface the reference reader/decoder touch
    (``buf[i]``, ``buf[a:b]``, ``buf.rfind``), decompressing blocks on demand
    through a bounded LRU - a lookup touches a few hundred KB, never the whole
    database.
    """

    def __init__(self, path: Any, cache_blocks: int = 64) -> None:
        self._fh = open(path, "rb")
        head = self._fh.read(_HEAD_FMT.size)
        if len(head) < _HEAD_FMT.size or head[:8] != _MAGIC:
            self._fh.close()
            raise InvalidDatabaseError(f"not a block container: {path}")
        _magic, self._block_size, self._count, self._raw_size = _HEAD_FMT.unpack(head)
        if self._block_size <= 0 or self._raw_size <= 0:
            self._fh.close()
            raise InvalidDatabaseError(f"corrupt block container header: {path}")
        raw_lengths = self._fh.read(self._count * _INDEX_FMT.size)
        if len(raw_lengths) != self._count * _INDEX_FMT.size:
            self._fh.close()
            raise InvalidDatabaseError(f"truncated block index in {path}")
        self._lens = [item[0] for item in _INDEX_FMT.iter_unpack(raw_lengths)]
        self._blob_offs = []
        cursor = self._fh.tell()
        for length in self._lens:
            self._blob_offs.append(cursor)
            cursor += length
        self._dctx = _zstd.ZstdDecompressor()
        self._cache: OrderedDict[int, bytes] = OrderedDict()
        self._cache_cap = max(8, cache_blocks)
        self._lock = _threading.Lock()

    # -- size ---------------------------------------------------------------
    def size(self) -> int:
        return self._raw_size

    def __len__(self) -> int:
        return self._raw_size

    # -- block read -----------------------------------------------------------
    def _block(self, index: int) -> bytes:
        with self._lock:
            cached = self._cache.get(index)
            if cached is not None:
                self._cache.move_to_end(index)
                return cached
            self._fh.seek(self._blob_offs[index])
            blob = self._fh.read(self._lens[index])
            data = self._dctx.decompress(blob)
            self._cache[index] = data
            if len(self._cache) > self._cache_cap:
                self._cache.popitem(last=False)
            return data

    def _bytes_range(self, start: int, length: int) -> bytes:
        if length <= 0:
            return b""
        out = bytearray()
        while length > 0:
            block_index = start // self._block_size
            block_offset = start % self._block_size
            block = self._block(block_index)
            take = min(len(block) - block_offset, length)
            if take <= 0:
                break
            out += block[block_offset:block_offset + take]
            start += take
            length -= take
        return bytes(out)

    # -- indexing ---------------------------------------------------------------
    def __getitem__(self, key: Any) -> Any:
        if isinstance(key, slice):
            start, stop, step = key.indices(self._raw_size)
            if step != 1:
                return bytes(self[i] for i in range(start, stop, step))
            return self._bytes_range(start, max(0, stop - start))
        if isinstance(key, int):
            return self._bytes_range(key, 1)[0]
        raise TypeError(f"LazyBuffer indices must be int or slice, not {type(key)}")

    def rfind(self, sub: bytes, start: int = 0, end: Optional[int] = None) -> int:
        end = self._raw_size if end is None else min(end, self._raw_size)
        start = max(0, start)
        if end <= start:
            return -1
        window = self._bytes_range(start, end - start)
        found = window.rfind(sub)
        return -1 if found < 0 else start + found

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:  # pragma: no cover - closing must never raise
            pass


class LazyReader(Reader):
    """:class:`Reader` backed by a ``.bz`` block container."""

    def _load_buffer(self, database: Any, mode: int = MODE_AUTO) -> str:
        self._buffer = database
        self._buffer_size = database.size()
        return "<lazy-bz>"


def open_lazy(path: Any, *, cache_blocks: int = 64) -> LazyReader:
    """Open a ``.bz`` database lazily (decompresses only touched blocks)."""
    buffer = LazyBuffer(path, cache_blocks=cache_blocks)
    try:
        return LazyReader(buffer, mode=MODE_MEMORY)
    except Exception:
        buffer.close()
        raise
