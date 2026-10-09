#!/usr/bin/env python3
"""A minimal HTTP server that imitates part of the AWS S3 API on a local directory."""

from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Set as AbstractSet
from datetime import datetime, timezone
from email.utils import formatdate, parsedate_to_datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit
from xml.sax.saxutils import escape

__version__ = "1.2.1"
BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
S3_NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"
TEMP_PREFIX = ".localbucket-"
MAX_KEYS_LIMIT = 1000
UPLOADS_DIR = ".localbucket-uploads"
UPLOAD_ID_RE = re.compile(r"^[0-9a-f]{32}$")
UPLOAD_META = "upload.json"
MIN_PART_SIZE = 5 * 1024 * 1024
MAX_PART_NUMBER = 10000
BLOCK_SIZE = 1024 * 1024
RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
ALWAYS_ALLOWED_QUERY = {
    "x-id",
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "AWSAccessKeyId",
    "Signature",
    "Expires",
}
LIST_OBJECTS_QUERY = {"prefix", "delimiter", "marker", "max-keys", "encoding-type"}
LIST_OBJECTS_V2_QUERY = {
    "list-type",
    "prefix",
    "delimiter",
    "max-keys",
    "start-after",
    "continuation-token",
    "encoding-type",
}
logger = logging.getLogger("localbucket")
RESPONSE_HEADER_OVERRIDES = {
    "response-cache-control": "Cache-Control",
    "response-content-disposition": "Content-Disposition",
    "response-content-encoding": "Content-Encoding",
    "response-content-language": "Content-Language",
    "response-content-type": "Content-Type",
    "response-expires": "Expires",
}
CORS_EXPOSE_HEADERS = (
    "Accept-Ranges, Content-Disposition, Content-Encoding, Content-Range, ETag, "
    "x-amz-bucket-region, x-amz-request-id"
)
BANNER = r"""
    __                     ______             __        __
   / /   ____  _________ _/ / __ )__  _______/ /_____  / /_
  / /   / __ \/ ___/ __ `/ / __  / / / / ___/ //_/ _ \/ __/
 / /___/ /_/ / /__/ /_/ / / /_/ / /_/ / /__/ ,< /  __/ /_
/_____/\____/\___/\__,_/_/_____/\__,_/\___/_/|_|\___/\__/
"""
HELP_EPILOG = """\
supported operations (path-style URLs only, credentials are not checked):
  ListBuckets, CreateBucket, DeleteBucket (empty buckets only), HeadBucket,
  ListObjects, ListObjectsV2, PutObject (with If-None-Match: *), CopyObject,
  GetObject and HeadObject (with Range and response-* header overrides),
  DeleteObject, CreateMultipartUpload, UploadPart, UploadPartCopy,
  CompleteMultipartUpload, AbortMultipartUpload,
  GetObjectTagging (always an empty tag set, because tags are not stored)
  CORS requests from any origin are allowed, including OPTIONS preflights.
  Unfinished multipart uploads are kept in DATA_ROOT/.localbucket-uploads.

example:
  localbucket ./data
  aws --endpoint-url http://127.0.0.1:9000 s3 cp file.txt s3://mybucket/
"""


class S3Error(Exception):
    """An S3-style error that is returned to the client as an XML document."""

    def __init__(self, status: HTTPStatus, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def read_chunked(stream) -> bytes:
    """Decode an HTTP chunked or aws-chunked body, dropping signatures and trailers."""
    body = bytearray()
    while True:
        line = stream.readline()
        if not line:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "IncompleteBody",
                "Chunked body ended unexpectedly.",
            )
        size_text = line.split(b";", 1)[0].strip()
        try:
            size = int(size_text, 16)
        except ValueError:
            raise S3Error(
                HTTPStatus.BAD_REQUEST, "InvalidRequest", "Invalid chunk size."
            ) from None
        if size == 0:
            while stream.readline().strip():
                pass
            return bytes(body)
        chunk = stream.read(size)
        if len(chunk) != size:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "IncompleteBody",
                "Chunk is shorter than declared.",
            )
        body += chunk
        stream.readline()


def iso_timestamp(epoch: float) -> str:
    """Format a Unix timestamp the way S3 formats dates in XML responses."""
    return datetime.fromtimestamp(epoch, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S.000Z"
    )


_etag_cache: dict[str, tuple[tuple[int, int, int, int], str]] = {}
_etag_lock = threading.Lock()


def stat_signature(path: Path) -> tuple[int, int, int, int]:
    """Return a file's identity, size and mtime, which change when it is rewritten."""
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns


def object_etag(path: Path) -> str:
    """Return a file's MD5 ETag, reusing the cached value while it is unchanged."""
    signature = stat_signature(path)
    with _etag_lock:
        cached = _etag_cache.get(str(path))
    if cached and cached[0] == signature:
        return cached[1]
    digest = hashlib.md5()
    with path.open("rb") as file:
        while block := file.read(1024 * 1024):
            digest.update(block)
    etag = digest.hexdigest()
    with _etag_lock:
        _etag_cache[str(path)] = (signature, etag)
    return etag


def remember_etag(path: Path, etag: str):
    """Cache the ETag of a file that has just been written."""
    signature = stat_signature(path)
    with _etag_lock:
        _etag_cache[str(path)] = (signature, etag)


def etag_matches(header: str, etag: str) -> bool:
    """Return whether an If-Match style header lists the given ETag or the wildcard."""
    candidates = {
        value.strip().removeprefix("W/").strip('"') for value in header.split(",")
    }
    return "*" in candidates or etag in candidates


def http_date(header: str | None) -> int | None:
    """Parse an HTTP date header to a Unix timestamp, or None if missing or invalid."""
    if not header:
        return None
    try:
        return int(parsedate_to_datetime(header).timestamp())
    except (TypeError, ValueError, IndexError):
        return None


def write_bytes(file, data: bytes) -> str:
    """Write data to a file and return its hex MD5 digest."""
    file.write(data)
    return hashlib.md5(data).hexdigest()


def copy_ranges(destination, ranges: list[tuple[Path, int, int]]) -> str:
    """Write (path, start, length) file ranges to destination; return their hex MD5."""
    digest = hashlib.md5()
    for source, start, length in ranges:
        with source.open("rb") as file:
            file.seek(start)
            remaining = length
            while remaining > 0:
                block = file.read(min(remaining, BLOCK_SIZE))
                if not block:
                    raise S3Error(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        "InternalError",
                        "A source file changed while it was being copied.",
                    )
                destination.write(block)
                digest.update(block)
                remaining -= len(block)
    return digest.hexdigest()


def parse_range(header: str, size: int) -> tuple[int, int] | None:
    """Return the Range header's inclusive byte range, or None for the whole object."""
    match = RANGE_RE.match(header.strip())
    if not match or match.groups() == ("", ""):
        return None
    first, last = match.groups()
    if first:
        start = int(first)
        if last and int(last) < start:
            return None
        end = min(int(last), size - 1) if last else size - 1
    else:
        length = int(last)
        if length == 0:
            raise invalid_range_error()
        start, end = max(size - length, 0), size - 1
    if start >= size:
        raise invalid_range_error()
    return start, end


def invalid_range_error() -> S3Error:
    """Build the error for a Range header that starts past the end of the object."""
    return S3Error(
        HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
        "InvalidRange",
        "The requested range is not satisfiable",
    )


def sub_element(parent: ET.Element, tag: str, text: str) -> ET.Element:
    """Add a child element containing the given text."""
    child = ET.SubElement(parent, tag)
    child.text = text
    return child


def bucket_dirs(data_root: Path) -> list[Path]:
    """Return the bucket directories under the data root, sorted by name."""
    return sorted(
        (
            entry
            for entry in data_root.iterdir()
            if entry.is_dir() and BUCKET_NAME_RE.match(entry.name)
        ),
        key=lambda p: p.name,
    )


def log_buckets(data_root: Path):
    """Log the buckets under the data root as a comma-separated list."""
    buckets = [entry.name for entry in bucket_dirs(data_root)]
    logger.info("Buckets: %s", ", ".join(buckets) or "none")


class S3Handler(BaseHTTPRequestHandler):
    """Handles path-style S3 requests for buckets and objects."""

    server_version = "LocalBucket"
    protocol_version = "HTTP/1.1"
    data_root: Path
    query: dict[str, str]

    def log_message(self, format: str, *args):
        """Send a request log line to the localbucket logger."""
        message = format % args
        control_chars = getattr(self, "_control_char_table", None)
        if control_chars:
            message = message.translate(control_chars)
        logger.info("%s %s", self.address_string(), message)

    def do_PUT(self):
        """Create a bucket or write an object."""
        self._dispatch(self._put)

    def do_GET(self):
        """List buckets, list a bucket's objects or read an object."""
        self._dispatch(self._get_request)

    def do_HEAD(self):
        """Check a bucket exists or return an object's metadata without its body."""
        self._dispatch(self._head_request)

    def do_POST(self):
        """Start or complete a multipart upload."""
        self._dispatch(self._post_request)

    def do_DELETE(self):
        """Delete an object or abort a multipart upload."""
        self._dispatch(self._delete_request)

    def do_OPTIONS(self):
        """Answer a CORS preflight request, allowing any origin, method and header."""
        headers = {
            "Access-Control-Allow-Methods": "GET, HEAD, PUT, POST, DELETE",
            "Access-Control-Max-Age": "3000",
        }
        requested = self.headers.get("Access-Control-Request-Headers")
        if requested:
            headers["Access-Control-Allow-Headers"] = requested
        self._send(HTTPStatus.OK, headers=headers)

    def _dispatch(self, action):
        """Parse bucket, key and query from the URL, run it and send errors as XML."""
        try:
            url = urlsplit(self.path)
            path = unquote(url.path)
            bucket, _, key = path.lstrip("/").partition("/")
            parsed = parse_qs(url.query, keep_blank_values=True)
            self.query = {name: values[0] for name, values in parsed.items()}
            action(bucket, key)
        except S3Error as error:
            self._send_error(error)

    def _get_request(self, bucket: str, key: str):
        """Route a GET request to ListBuckets, ListObjects(V2) or GetObject."""
        if not bucket:
            self._check_query("ListBuckets")
            self._list_buckets()
        elif not key:
            if self.query.get("list-type") == "2":
                self._check_query("ListObjectsV2", LIST_OBJECTS_V2_QUERY)
                self._list_objects_v2(bucket)
            else:
                self._check_query("ListObjects", LIST_OBJECTS_QUERY)
                self._list_objects(bucket)
        elif "tagging" in self.query:
            self._check_query("GetObjectTagging", {"tagging"})
            if not self._object_path(bucket, key).is_file():
                raise S3Error(
                    HTTPStatus.NOT_FOUND,
                    "NoSuchKey",
                    "The specified key does not exist.",
                )
            root = ET.Element("Tagging", xmlns=S3_NAMESPACE)
            ET.SubElement(root, "TagSet")
            self._send_xml(root)
        else:
            self._check_query("GetObject", RESPONSE_HEADER_OVERRIDES.keys())
            self._get(bucket, key, include_body=True)

    def _head_request(self, bucket: str, key: str):
        """Route a HEAD request to HeadBucket or HeadObject."""
        if not bucket:
            raise self._not_implemented_error()
        if not key:
            self._check_query("HeadBucket")
            self._bucket_dir(bucket)
            self._send(HTTPStatus.OK, headers={"x-amz-bucket-region": "us-east-1"})
        else:
            self._check_query("HeadObject", RESPONSE_HEADER_OVERRIDES.keys())
            self._get(bucket, key, include_body=False)

    def _delete_request(self, bucket: str, key: str):
        """Route DELETE to DeleteBucket, DeleteObject or AbortMultipartUpload."""
        if not bucket:
            raise self._not_implemented_error()
        if not key:
            self._check_query("DeleteBucket")
            self._delete_bucket(bucket)
        elif "uploadId" in self.query:
            self._check_query("AbortMultipartUpload", {"uploadId"})
            shutil.rmtree(self._upload_dir(bucket, key), ignore_errors=True)
            self._send(HTTPStatus.NO_CONTENT)
        else:
            self._check_query("DeleteObject")
            self._delete_object(bucket, key)

    def _post_request(self, bucket: str, key: str):
        """Route a POST request to CreateMultipartUpload or CompleteMultipartUpload."""
        if not bucket or not key:
            raise self._not_implemented_error()
        if "uploads" in self.query:
            self._check_query("CreateMultipartUpload", {"uploads"})
            self._read_body()
            self._create_multipart_upload(bucket, key)
        elif "uploadId" in self.query:
            self._check_query("CompleteMultipartUpload", {"uploadId"})
            self._complete_multipart_upload(bucket, key, self._read_body())
        else:
            raise self._not_implemented_error()

    def _check_query(self, operation: str, allowed: AbstractSet[str] = frozenset()):
        """Raise NotImplemented for query parameters the operation does not support."""
        unsupported = sorted(set(self.query) - ALWAYS_ALLOWED_QUERY - allowed)
        if unsupported:
            self.log_message(
                "rejected %s with unsupported query parameters: %s",
                operation,
                ", ".join(unsupported),
            )
            raise self._not_implemented_error()

    def _list_buckets(self):
        """Send the list of all buckets under the data root."""
        root = ET.Element("ListAllMyBucketsResult", xmlns=S3_NAMESPACE)
        owner = ET.SubElement(root, "Owner")
        sub_element(owner, "ID", "localbucket")
        sub_element(owner, "DisplayName", "LocalBucket")
        buckets = ET.SubElement(root, "Buckets")
        for entry in bucket_dirs(self.data_root):
            stat = entry.stat()
            created = getattr(stat, "st_birthtime", stat.st_ctime)
            bucket = ET.SubElement(buckets, "Bucket")
            sub_element(bucket, "Name", entry.name)
            sub_element(bucket, "CreationDate", iso_timestamp(created))
        self._send_xml(root)

    def _list_objects(self, bucket: str):
        """Send one page of objects, with prefix, delimiter, marker and paging."""
        bucket_dir = self._bucket_dir(bucket)
        prefix = self.query.get("prefix", "")
        delimiter = self.query.get("delimiter", "")
        marker = self.query.get("marker", "")
        max_keys = self._max_keys()
        marker_is_prefix = bool(delimiter) and marker.endswith(delimiter)
        contents, common_prefixes, truncated, last_entry = self._list_entries(
            bucket_dir, prefix, delimiter, marker, marker_is_prefix, max_keys
        )

        encode = self._list_encoder()
        root = ET.Element("ListBucketResult", xmlns=S3_NAMESPACE)
        sub_element(root, "Name", bucket)
        sub_element(root, "Prefix", encode(prefix))
        sub_element(root, "Marker", encode(marker))
        if delimiter:
            sub_element(root, "Delimiter", encode(delimiter))
        sub_element(root, "MaxKeys", str(max_keys))
        sub_element(root, "IsTruncated", "true" if truncated else "false")
        if truncated:
            sub_element(root, "NextMarker", encode(last_entry[2:]))
        if self.query.get("encoding-type") == "url":
            sub_element(root, "EncodingType", "url")
        self._add_list_entries(root, contents, common_prefixes, encode)
        self._send_xml(root)

    def _list_objects_v2(self, bucket: str):
        """Send one page of objects, with prefix, delimiter, start-after and paging."""
        bucket_dir = self._bucket_dir(bucket)
        prefix = self.query.get("prefix", "")
        delimiter = self.query.get("delimiter", "")
        start_after = self.query.get("start-after", "")
        token = self.query.get("continuation-token")
        max_keys = self._max_keys()

        marker, marker_is_prefix = start_after, False
        if token is not None:
            try:
                decoded = base64.urlsafe_b64decode(token.encode()).decode()
            except ValueError:
                decoded = ""
            if decoded[:2] not in ("K:", "P:"):
                raise S3Error(
                    HTTPStatus.BAD_REQUEST,
                    "InvalidArgument",
                    "The continuation token provided is incorrect.",
                )
            marker, marker_is_prefix = decoded[2:], decoded.startswith("P:")
        contents, common_prefixes, truncated, last_entry = self._list_entries(
            bucket_dir, prefix, delimiter, marker, marker_is_prefix, max_keys
        )

        encode = self._list_encoder()
        root = ET.Element("ListBucketResult", xmlns=S3_NAMESPACE)
        sub_element(root, "Name", bucket)
        sub_element(root, "Prefix", encode(prefix))
        if delimiter:
            sub_element(root, "Delimiter", encode(delimiter))
        sub_element(root, "MaxKeys", str(max_keys))
        sub_element(root, "KeyCount", str(len(contents) + len(common_prefixes)))
        sub_element(root, "IsTruncated", "true" if truncated else "false")
        if token is not None:
            sub_element(root, "ContinuationToken", token)
        if truncated:
            next_token = base64.urlsafe_b64encode(last_entry.encode()).decode()
            sub_element(root, "NextContinuationToken", next_token)
        if start_after:
            sub_element(root, "StartAfter", encode(start_after))
        if self.query.get("encoding-type") == "url":
            sub_element(root, "EncodingType", "url")
        self._add_list_entries(root, contents, common_prefixes, encode)
        self._send_xml(root)

    def _max_keys(self) -> int:
        """Return the max-keys query parameter, capped at the S3 limit of 1000."""
        try:
            max_keys = min(
                int(self.query.get("max-keys", MAX_KEYS_LIMIT)), MAX_KEYS_LIMIT
            )
        except ValueError:
            max_keys = -1
        if max_keys < 0:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidArgument",
                "Provided max-keys not an integer or within integer range.",
            )
        return max_keys

    def _list_encoder(self):
        """Return a function that URL-encodes listed names if encoding-type=url."""
        if self.query.get("encoding-type") == "url":
            return lambda text: quote(text, safe="/")
        return lambda text: text

    def _list_entries(
        self,
        bucket_dir: Path,
        prefix: str,
        delimiter: str,
        marker: str,
        marker_is_prefix: bool,
        max_keys: int,
    ) -> tuple[list[tuple[str, Path]], list[str], bool, str]:
        """Return one page of keys and common prefixes that come after the marker."""
        contents: list[tuple[str, Path]] = []
        common_prefixes: list[str] = []
        last_entry = ""
        truncated = False
        keys = []
        for directory, _, files in os.walk(bucket_dir):
            for name in files:
                if name.startswith(TEMP_PREFIX):
                    continue
                path = Path(directory, name)
                keys.append((path.relative_to(bucket_dir).as_posix(), path))
        keys.sort(key=lambda item: item[0].encode())
        for key, path in keys:
            if not key.startswith(prefix):
                continue
            if marker and (
                key.encode() <= marker.encode()
                or (marker_is_prefix and key.startswith(marker))
            ):
                continue
            common = ""
            if delimiter:
                index = key.find(delimiter, len(prefix))
                if index >= 0:
                    common = key[: index + len(delimiter)]
            if common and common_prefixes and common_prefixes[-1] == common:
                continue
            if len(contents) + len(common_prefixes) >= max_keys:
                truncated = max_keys > 0
                break
            if common:
                common_prefixes.append(common)
                last_entry = "P:" + common
            else:
                contents.append((key, path))
                last_entry = "K:" + key
        return contents, common_prefixes, truncated, last_entry

    def _add_list_entries(
        self,
        root: ET.Element,
        contents: list[tuple[str, Path]],
        common_prefixes: list[str],
        encode,
    ):
        """Add Contents and CommonPrefixes elements to a listing result."""
        for key, path in contents:
            item = ET.SubElement(root, "Contents")
            stat = path.stat()
            sub_element(item, "Key", encode(key))
            sub_element(item, "LastModified", iso_timestamp(stat.st_mtime))
            sub_element(item, "ETag", f'"{object_etag(path)}"')
            sub_element(item, "Size", str(stat.st_size))
            sub_element(item, "StorageClass", "STANDARD")
        for common in common_prefixes:
            item = ET.SubElement(root, "CommonPrefixes")
            sub_element(item, "Prefix", encode(common))

    def _delete_object(self, bucket: str, key: str):
        """Delete an object if it exists and remove any folders left empty."""
        bucket_dir = self._bucket_dir(bucket).resolve()
        target = self._object_path(bucket, key)
        if target.is_file():
            target.unlink(missing_ok=True)
            parent = target.parent
            while parent != bucket_dir:
                try:
                    parent.rmdir()
                except OSError:
                    break
                parent = parent.parent
        self._send(HTTPStatus.NO_CONTENT)

    def _delete_bucket(self, bucket: str):
        """Delete an empty bucket, or raise BucketNotEmpty if it holds any files."""
        bucket_dir = self._bucket_dir(bucket)
        not_empty = S3Error(
            HTTPStatus.CONFLICT,
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
        )
        directories = []
        for directory, _, files in os.walk(bucket_dir, topdown=False):
            if any(not name.startswith(TEMP_PREFIX) for name in files):
                raise not_empty
            directories.append(directory)
        for directory in directories:
            try:
                os.rmdir(directory)
            except OSError:
                raise not_empty from None
        self._delete_bucket_uploads(bucket)
        self._send(HTTPStatus.NO_CONTENT)
        log_buckets(self.data_root)

    def _delete_bucket_uploads(self, bucket: str):
        """Remove the unfinished multipart uploads that belong to a bucket."""
        uploads_root = self.data_root / UPLOADS_DIR
        if not uploads_root.is_dir():
            return
        for upload_dir in uploads_root.iterdir():
            try:
                meta = json.loads((upload_dir / UPLOAD_META).read_text())
            except (OSError, ValueError):
                continue
            if isinstance(meta, dict) and meta.get("bucket") == bucket:
                shutil.rmtree(upload_dir, ignore_errors=True)

    def _put(self, bucket: str, key: str):
        """Route PUT to CreateBucket, PutObject, CopyObject or UploadPart(Copy)."""
        if not bucket:
            raise self._not_implemented_error()
        copy_source = "x-amz-copy-source" in self.headers
        if key and ("uploadId" in self.query or "partNumber" in self.query):
            self._check_query(
                "UploadPartCopy" if copy_source else "UploadPart",
                {"uploadId", "partNumber"},
            )
            body = self._read_body()
            if copy_source:
                self._upload_part_copy(bucket, key)
            else:
                etag = self._store_part(
                    bucket, key, lambda file: write_bytes(file, body)
                )
                self._send(HTTPStatus.OK, headers={"ETag": f'"{etag}"'})
            return
        self._check_query(
            ("CopyObject" if copy_source else "PutObject") if key else "CreateBucket"
        )
        body = self._read_body()
        if not key:
            self._create_bucket(bucket)
        elif copy_source:
            self._copy_object(bucket, key)
        else:
            etag = self._write_object(bucket, key, lambda file: write_bytes(file, body))
            self._send(HTTPStatus.OK, headers={"ETag": f'"{etag}"'})

    def _create_bucket(self, bucket: str):
        """Create the bucket directory under the data root."""
        if not BUCKET_NAME_RE.match(bucket) or ".." in bucket:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidBucketName",
                "The specified bucket is not valid.",
            )
        bucket_dir = self.data_root / bucket
        try:
            bucket_dir.mkdir()
        except FileExistsError:
            raise S3Error(
                HTTPStatus.CONFLICT,
                "BucketAlreadyOwnedByYou",
                "Your previous request to create the named bucket succeeded and you "
                "already own it.",
            ) from None
        self._send(HTTPStatus.OK, headers={"Location": f"/{bucket}"})
        log_buckets(self.data_root)

    def _copy_object(self, bucket: str, key: str):
        """Copy the object named in the x-amz-copy-source header to this key."""
        source = self._copy_source()
        target = self._object_path(bucket, key)
        directive = self.headers.get("x-amz-metadata-directive", "COPY").strip().upper()
        if source == target and directive != "REPLACE":
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidRequest",
                "This copy request is illegal because it is trying to copy an object "
                "to itself without changing the object's metadata, storage class, "
                "website redirect location or encryption attributes.",
            )
        ranges = [(source, 0, source.stat().st_size)]
        etag = self._write_object(bucket, key, lambda file: copy_ranges(file, ranges))
        root = ET.Element("CopyObjectResult", xmlns=S3_NAMESPACE)
        sub_element(root, "LastModified", iso_timestamp(target.stat().st_mtime))
        sub_element(root, "ETag", f'"{etag}"')
        self._send_xml(root)

    def _copy_source(self) -> Path:
        """Return the file named by x-amz-copy-source, rejecting unsupported options."""
        source, _, source_query = self.headers["x-amz-copy-source"].partition("?")
        if source_query:
            raise self._not_implemented_error()
        bucket, _, key = unquote(source).lstrip("/").partition("/")
        if not bucket or not key:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidArgument",
                "Copy Source must mention the source bucket and key: "
                "sourcebucket/sourcekey",
            )
        path = self._object_path(bucket, key)
        if not path.is_file():
            raise S3Error(
                HTTPStatus.NOT_FOUND, "NoSuchKey", "The specified key does not exist."
            )
        self._check_copy_conditions(path)
        return path

    def _check_copy_conditions(self, source: Path):
        """Raise PreconditionFailed unless the x-amz-copy-source-if-* headers hold."""
        if_match = self.headers.get("x-amz-copy-source-if-match")
        if_none_match = self.headers.get("x-amz-copy-source-if-none-match")
        modified_since = http_date(
            self.headers.get("x-amz-copy-source-if-modified-since")
        )
        unmodified_since = http_date(
            self.headers.get("x-amz-copy-source-if-unmodified-since")
        )
        etag = object_etag(source)
        modified = int(source.stat().st_mtime)
        if if_match is not None:
            if not etag_matches(if_match, etag):
                raise self._precondition_failed_error()
        elif unmodified_since is not None and modified > unmodified_since:
            raise self._precondition_failed_error()
        if if_none_match is not None:
            if etag_matches(if_none_match, etag):
                raise self._precondition_failed_error()
        elif modified_since is not None and modified <= modified_since:
            raise self._precondition_failed_error()

    def _create_multipart_upload(self, bucket: str, key: str):
        """Start a multipart upload and send its upload ID."""
        self._object_path(bucket, key)
        upload_id = uuid.uuid4().hex
        upload_dir = self.data_root / UPLOADS_DIR / upload_id
        upload_dir.mkdir(parents=True)
        (upload_dir / UPLOAD_META).write_text(
            json.dumps({"bucket": bucket, "key": key})
        )
        root = ET.Element("InitiateMultipartUploadResult", xmlns=S3_NAMESPACE)
        sub_element(root, "Bucket", bucket)
        sub_element(root, "Key", key)
        sub_element(root, "UploadId", upload_id)
        self._send_xml(root)

    def _upload_part_copy(self, bucket: str, key: str):
        """Store all or part of an existing object as one part of a multipart upload."""
        source = self._copy_source()
        size = source.stat().st_size
        header = self.headers.get("x-amz-copy-source-range")
        if header is None:
            start, length = 0, size
        else:
            match = RANGE_RE.match(header.strip())
            if (
                not match
                or not all(match.groups())
                or int(match[1]) > int(match[2])
                or int(match[2]) >= size
            ):
                raise S3Error(
                    HTTPStatus.BAD_REQUEST,
                    "InvalidArgument",
                    f"Range specified is not valid for source object of size: {size}",
                )
            start, length = int(match[1]), int(match[2]) - int(match[1]) + 1
        etag = self._store_part(
            bucket, key, lambda file: copy_ranges(file, [(source, start, length)])
        )
        root = ET.Element("CopyPartResult", xmlns=S3_NAMESPACE)
        sub_element(root, "LastModified", iso_timestamp(time.time()))
        sub_element(root, "ETag", f'"{etag}"')
        self._send_xml(root)

    def _complete_multipart_upload(self, bucket: str, key: str, body: bytes):
        """Join the listed parts into the final object and delete the upload."""
        upload_dir = self._upload_dir(bucket, key)
        try:
            parts = ET.fromstring(body).findall("{*}Part")
        except ET.ParseError:
            parts = []
        if not parts:
            raise self._malformed_xml_error()
        ranges = []
        previous = 0
        for part in parts:
            try:
                number = int(part.findtext("{*}PartNumber") or "")
            except ValueError:
                raise self._malformed_xml_error() from None
            if number <= previous:
                raise S3Error(
                    HTTPStatus.BAD_REQUEST,
                    "InvalidPartOrder",
                    "The list of parts was not in ascending order. The parts list "
                    "must be specified in order by part number.",
                )
            previous = number
            path = upload_dir / f"{number:05d}"
            etag = (part.findtext("{*}ETag") or "").strip().strip('"')
            if not path.is_file() or etag != object_etag(path):
                raise S3Error(
                    HTTPStatus.BAD_REQUEST,
                    "InvalidPart",
                    "One or more of the specified parts could not be found. The part "
                    "may not have been uploaded, or the specified entity tag may not "
                    "match the part's entity tag.",
                )
            ranges.append((path, 0, path.stat().st_size))
        if any(length < MIN_PART_SIZE for _, _, length in ranges[:-1]):
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "EntityTooSmall",
                "Your proposed upload is smaller than the minimum allowed object size.",
            )
        etag = self._write_object(bucket, key, lambda file: copy_ranges(file, ranges))
        shutil.rmtree(upload_dir, ignore_errors=True)
        root = ET.Element("CompleteMultipartUploadResult", xmlns=S3_NAMESPACE)
        location = f"http://{self.headers.get('Host', '')}/{quote(bucket)}/{quote(key)}"
        sub_element(root, "Location", location)
        sub_element(root, "Bucket", bucket)
        sub_element(root, "Key", key)
        sub_element(root, "ETag", f'"{etag}"')
        self._send_xml(root)

    def _upload_dir(self, bucket: str, key: str) -> Path:
        """Return the uploadId's folder, checking the upload belongs to this key."""
        self._bucket_dir(bucket)
        upload_id = self.query.get("uploadId", "")
        upload_dir = self.data_root / UPLOADS_DIR / upload_id
        meta = None
        if UPLOAD_ID_RE.match(upload_id):
            try:
                meta = json.loads((upload_dir / UPLOAD_META).read_text())
            except (OSError, ValueError):
                meta = None
        if (
            not isinstance(meta, dict)
            or meta.get("bucket") != bucket
            or meta.get("key") != key
        ):
            raise self._no_such_upload_error()
        return upload_dir

    def _store_part(self, bucket: str, key: str, write) -> str:
        """Write one multipart upload part via a temporary file and return its ETag."""
        upload_dir = self._upload_dir(bucket, key)
        try:
            number = int(self.query.get("partNumber", ""))
        except ValueError:
            number = 0
        if not 1 <= number <= MAX_PART_NUMBER:
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidArgument",
                f"Part number must be an integer between 1 and {MAX_PART_NUMBER}, "
                "inclusive",
            )
        part_path = upload_dir / f"{number:05d}"
        try:
            fd, tmp_name = tempfile.mkstemp(dir=upload_dir, prefix=TEMP_PREFIX)
        except FileNotFoundError:
            raise self._no_such_upload_error() from None
        try:
            with os.fdopen(fd, "wb") as tmp:
                etag = write(tmp)
            try:
                os.replace(tmp_name, part_path)
            except FileNotFoundError:
                raise self._no_such_upload_error() from None
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
        remember_etag(part_path, etag)
        return etag

    def _write_object(self, bucket: str, key: str, write) -> str:
        """Write an object honouring If-None-Match and return its ETag."""
        target = self._object_path(bucket, key)
        if_none_match = self.headers.get("If-None-Match", "").strip() == "*"
        if if_none_match and target.exists():
            raise self._precondition_failed_error()
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=target.parent, prefix=TEMP_PREFIX)
        try:
            with os.fdopen(fd, "wb") as tmp:
                etag = write(tmp)
            if if_none_match:
                try:
                    os.link(tmp_name, target)
                except FileExistsError:
                    raise self._precondition_failed_error() from None
                os.unlink(tmp_name)
            else:
                os.replace(tmp_name, target)
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise
        remember_etag(target, etag)
        return etag

    def _get(self, bucket: str, key: str, include_body: bool):
        """Send the object's content and metadata, or just the requested byte range."""
        if not bucket or not key:
            raise self._not_implemented_error()
        overrides = self._response_header_overrides()
        target = self._object_path(bucket, key)
        if not target.is_file():
            raise S3Error(
                HTTPStatus.NOT_FOUND, "NoSuchKey", "The specified key does not exist."
            )
        try:
            file = target.open("rb")
        except FileNotFoundError:
            raise S3Error(
                HTTPStatus.NOT_FOUND, "NoSuchKey", "The specified key does not exist."
            ) from None
        with file:
            stat = os.fstat(file.fileno())
            content_type = (
                mimetypes.guess_type(target.name)[0] or "application/octet-stream"
            )
            headers = {
                "Content-Type": content_type,
                "ETag": f'"{object_etag(target)}"',
                "Last-Modified": formatdate(stat.st_mtime, usegmt=True),
                "Accept-Ranges": "bytes",
                **overrides,
            }
            status, start, length = HTTPStatus.OK, 0, stat.st_size
            byte_range = parse_range(self.headers.get("Range", ""), stat.st_size)
            if byte_range:
                start, end = byte_range
                status, length = HTTPStatus.PARTIAL_CONTENT, end - start + 1
                headers["Content-Range"] = f"bytes {start}-{end}/{stat.st_size}"
            self._send_file(status, file, start, length, headers, include_body)

    def _response_header_overrides(self) -> dict[str, str]:
        """Return the headers that response-* query parameters ask to override."""
        overrides = {}
        for name, header in RESPONSE_HEADER_OVERRIDES.items():
            value = self.query.get(name)
            if value is None:
                continue
            if any(ord(char) < 32 or ord(char) == 127 for char in value):
                raise S3Error(
                    HTTPStatus.BAD_REQUEST,
                    "InvalidArgument",
                    f"Invalid value for {name}.",
                )
            overrides[header] = value
        return overrides

    def _not_implemented_error(self) -> S3Error:
        """Build the error returned for unsupported operations."""
        return S3Error(
            HTTPStatus.NOT_IMPLEMENTED,
            "NotImplemented",
            "A header or operation you provided implies functionality that is not "
            "implemented.",
        )

    def _precondition_failed_error(self) -> S3Error:
        """Build the error for a conditional write that finds an existing object."""
        return S3Error(
            HTTPStatus.PRECONDITION_FAILED,
            "PreconditionFailed",
            "At least one of the pre-conditions you specified did not hold",
        )

    def _malformed_xml_error(self) -> S3Error:
        """Build the error for a request body that is not the expected XML."""
        return S3Error(
            HTTPStatus.BAD_REQUEST,
            "MalformedXML",
            "The XML you provided was not well-formed or did not validate against "
            "our published schema",
        )

    def _no_such_upload_error(self) -> S3Error:
        """Build the error for an upload ID with no active upload for this key."""
        return S3Error(
            HTTPStatus.NOT_FOUND,
            "NoSuchUpload",
            "The specified upload does not exist. The upload ID may be invalid, or "
            "the upload may have been aborted or completed.",
        )

    def _bucket_dir(self, bucket: str) -> Path:
        """Return a bucket's directory, raising NoSuchBucket if it does not exist."""
        bucket_dir = self.data_root / bucket
        if not BUCKET_NAME_RE.match(bucket) or not bucket_dir.is_dir():
            raise S3Error(
                HTTPStatus.NOT_FOUND,
                "NoSuchBucket",
                "The specified bucket does not exist.",
            )
        return bucket_dir

    def _object_path(self, bucket: str, key: str) -> Path:
        """Resolve a key's path, checking the bucket exists and the key stays inside."""
        bucket_dir = self._bucket_dir(bucket)
        parts = key.split("/")
        if key.endswith("/") or any(part in ("", ".", "..") for part in parts):
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidArgument",
                "This server does not support this key.",
            )
        target = bucket_dir.joinpath(*parts).resolve()
        if not target.is_relative_to(bucket_dir.resolve()):
            raise S3Error(
                HTTPStatus.BAD_REQUEST,
                "InvalidArgument",
                "This server does not support this key.",
            )
        return target

    def _read_body(self) -> bytes:
        """Read the request body, decoding HTTP chunked and aws-chunked encodings."""
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            body = read_chunked(self.rfile)
        else:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
        content_sha = self.headers.get("x-amz-content-sha256", "")
        content_encoding = self.headers.get("Content-Encoding", "")
        if content_sha.startswith("STREAMING-") or "aws-chunked" in content_encoding:
            body = read_chunked(io.BytesIO(body))
        return body

    def _send_headers(self, status: HTTPStatus, length: int, headers=None):
        """Send the status line and headers, including the standard S3 ones."""
        self.send_response(status)
        self.send_header("x-amz-request-id", uuid.uuid4().hex.upper()[:16])
        self.send_header("Content-Length", str(length))
        origin = self.headers.get("Origin")
        if origin:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Expose-Headers", CORS_EXPOSE_HEADERS)
            self.send_header("Vary", "Origin")
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()

    def _send(
        self,
        status: HTTPStatus,
        body: bytes = b"",
        headers=None,
        include_body: bool = True,
    ):
        """Send a response with the standard S3 headers."""
        self._send_headers(status, len(body), headers)
        if include_body and body:
            self.wfile.write(body)

    def _send_file(
        self,
        status: HTTPStatus,
        file,
        start: int,
        length: int,
        headers,
        include_body: bool,
    ):
        """Send a response whose body is a byte range of an open file."""
        self._send_headers(status, length, headers)
        if not include_body:
            return
        file.seek(start)
        remaining = length
        while remaining > 0:
            block = file.read(min(remaining, BLOCK_SIZE))
            if not block:
                self.close_connection = True
                return
            self.wfile.write(block)
            remaining -= len(block)

    def _send_xml(self, root: ET.Element):
        """Send a successful XML response."""
        body = ET.tostring(root, encoding="UTF-8", xml_declaration=True)
        self._send(HTTPStatus.OK, body, {"Content-Type": "application/xml"})

    def _send_error(self, error: S3Error):
        """Send an S3-style XML error document."""
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>\n'
            f"<Error><Code>{error.code}</Code>"
            f"<Message>{escape(error.message)}</Message>"
            f"<Resource>{escape(urlsplit(self.path).path)}</Resource></Error>"
        ).encode()
        self.close_connection = True
        self._send(
            error.status,
            xml,
            {"Content-Type": "application/xml", "Connection": "close"},
            include_body=self.command != "HEAD",
        )


def main():
    """Parse the command line and run the server."""
    parser = argparse.ArgumentParser(
        prog="localbucket",
        description=(
            f"LocalBucket {__version__}: a minimal S3-compatible HTTP server that "
            f"stores buckets as folders in DATA_ROOT."
        ),
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "data_root",
        metavar="DATA_ROOT",
        type=Path,
        help="directory that holds the buckets",
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="address to listen on (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", type=int, default=9000, help="port to listen on (default: 9000)"
    )
    parser.add_argument(
        "--version", action="version", version=f"LocalBucket {__version__}"
    )
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(2)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s]: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    data_root = args.data_root.resolve()
    data_root.mkdir(parents=True, exist_ok=True)
    S3Handler.data_root = data_root

    server = ThreadingHTTPServer((args.host, args.port), S3Handler)
    print(BANNER, file=sys.stderr)
    logger.info(
        "LocalBucket %s serving %s on http://%s:%s",
        __version__,
        data_root,
        args.host,
        args.port,
    )
    log_buckets(data_root)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
