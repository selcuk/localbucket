#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "boto3",
# ]
# ///
"""Start localbucket on a temporary directory and check its S3 operations with boto3."""

from __future__ import annotations

import http.client
import io
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

LOCALBUCKET = Path(__file__).resolve().parent / "localbucket.py"
BUCKET = "testbucket"
KEY = "folder/hello.txt"
CONTENT = b"Hello from boto3!\n"
LIST_BUCKET = "listbucket"
TRANSFER_BUCKET = "transferbucket"
DELETE_BUCKET = "deletebucket"
PART_SIZE = 5 * 1024 * 1024
LIST_KEYS = [
    "a.txt",
    "dir/one.txt",
    "dir/two.txt",
    "dir/sub/three.txt",
    "dir-x.txt",
    "with space+plus.txt",
    "unicodé.txt",
    "z/deep/file.txt",
]


def free_port() -> int:
    """Return a TCP port that is currently free on localhost."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_port(port: int, timeout: float = 10.0):
    """Wait until something is listening on the given localhost port."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"localbucket did not start listening on port {port}")


def expect_error(call, status: int, code: str | None = None):
    """Assert the call fails with the given HTTP status and optional S3 error code."""
    try:
        call()
    except ClientError as error:
        actual = error.response["ResponseMetadata"]["HTTPStatusCode"]
        assert actual == status, f"expected HTTP {status}, got {actual}"
        actual_code = error.response["Error"]["Code"]
        assert code is None or actual_code == code, (
            f"expected error {code}, got {actual_code}"
        )
    else:
        raise AssertionError(f"expected HTTP {status}, but the call succeeded")


def list_all(s3, page_size: int, **kwargs) -> list[str]:
    """Collect every key and common prefix from a paginated ListObjectsV2 call."""
    paginator = s3.get_paginator("list_objects_v2")
    entries = []
    for page in paginator.paginate(
        Bucket=LIST_BUCKET, PaginationConfig={"PageSize": page_size}, **kwargs
    ):
        entries += [item["Key"] for item in page.get("Contents", [])]
        entries += [item["Prefix"] for item in page.get("CommonPrefixes", [])]
    return entries


def check_list_objects(s3):
    """Check ListObjectsV2 prefixes, delimiters, start-after, paging and odd keys."""
    for key in LIST_KEYS:
        s3.put_object(Bucket=LIST_BUCKET, Key=key, Body=key.encode())

    response = s3.list_objects_v2(Bucket=LIST_BUCKET)
    keys = [item["Key"] for item in response["Contents"]]
    assert keys == sorted(LIST_KEYS, key=str.encode), f"unexpected keys {keys}"
    assert response["KeyCount"] == len(LIST_KEYS) and not response["IsTruncated"]
    sizes = {item["Key"]: item["Size"] for item in response["Contents"]}
    assert all(sizes[key] == len(key.encode()) for key in LIST_KEYS), (
        f"unexpected sizes {sizes}"
    )
    print("listed all objects")

    response = s3.list_objects_v2(Bucket=LIST_BUCKET, Delimiter="/")
    keys = [item["Key"] for item in response["Contents"]]
    prefixes = [item["Prefix"] for item in response["CommonPrefixes"]]
    assert keys == ["a.txt", "dir-x.txt", "unicodé.txt", "with space+plus.txt"], (
        f"unexpected keys {keys}"
    )
    assert prefixes == ["dir/", "z/"], f"unexpected prefixes {prefixes}"
    print("listed top level with a delimiter")

    response = s3.list_objects_v2(Bucket=LIST_BUCKET, Prefix="dir/", Delimiter="/")
    keys = [item["Key"] for item in response["Contents"]]
    prefixes = [item["Prefix"] for item in response["CommonPrefixes"]]
    assert keys == ["dir/one.txt", "dir/two.txt"], f"unexpected keys {keys}"
    assert prefixes == ["dir/sub/"], f"unexpected prefixes {prefixes}"
    print("listed a prefix with a delimiter")

    response = s3.list_objects_v2(Bucket=LIST_BUCKET, StartAfter="dir/two.txt")
    keys = [item["Key"] for item in response["Contents"]]
    assert keys == ["unicodé.txt", "with space+plus.txt", "z/deep/file.txt"], (
        f"unexpected keys {keys}"
    )
    print("listed with start-after")

    response = s3.list_objects_v2(Bucket=LIST_BUCKET, MaxKeys=3)
    assert (
        response["KeyCount"] == 3
        and response["IsTruncated"]
        and response["NextContinuationToken"]
    )
    assert sorted(list_all(s3, 3)) == sorted(LIST_KEYS), "paging lost or repeated keys"
    paged = list_all(s3, 2, Delimiter="/")
    assert sorted(paged) == [
        "a.txt",
        "dir-x.txt",
        "dir/",
        "unicodé.txt",
        "with space+plus.txt",
        "z/",
    ], paged
    print("paged through objects with and without a delimiter")

    expect_error(lambda: s3.list_objects_v2(Bucket="missing-bucket"), 404)
    print("listing a missing bucket returns 404")


def list_all_v1(s3, page_size: int, **kwargs) -> list[str]:
    """Collect every key and common prefix from a paginated ListObjects call."""
    paginator = s3.get_paginator("list_objects")
    entries = []
    for page in paginator.paginate(
        Bucket=LIST_BUCKET, PaginationConfig={"PageSize": page_size}, **kwargs
    ):
        entries += [item["Key"] for item in page.get("Contents", [])]
        entries += [item["Prefix"] for item in page.get("CommonPrefixes", [])]
    return entries


def check_list_objects_v1(s3):
    """Check ListObjects (v1) prefixes, delimiters, markers and paging."""
    response = s3.list_objects(Bucket=LIST_BUCKET)
    keys = [item["Key"] for item in response["Contents"]]
    assert keys == sorted(LIST_KEYS, key=str.encode), f"unexpected keys {keys}"
    assert not response["IsTruncated"]
    print("listed all objects with ListObjects")

    response = s3.list_objects(Bucket=LIST_BUCKET, Prefix="dir/", Delimiter="/")
    keys = [item["Key"] for item in response["Contents"]]
    prefixes = [item["Prefix"] for item in response["CommonPrefixes"]]
    assert keys == ["dir/one.txt", "dir/two.txt"], f"unexpected keys {keys}"
    assert prefixes == ["dir/sub/"], f"unexpected prefixes {prefixes}"
    response = s3.list_objects(Bucket=LIST_BUCKET, Marker="dir/two.txt")
    keys = [item["Key"] for item in response["Contents"]]
    assert keys == ["unicodé.txt", "with space+plus.txt", "z/deep/file.txt"], (
        f"unexpected keys {keys}"
    )
    print("listed with a prefix, a delimiter and a marker using ListObjects")

    response = s3.list_objects(Bucket=LIST_BUCKET, MaxKeys=3)
    assert len(response["Contents"]) == 3 and response["IsTruncated"]
    assert sorted(list_all_v1(s3, 3)) == sorted(LIST_KEYS), (
        "paging lost or repeated keys"
    )
    paged = list_all_v1(s3, 2, Delimiter="/")
    assert sorted(paged) == [
        "a.txt",
        "dir-x.txt",
        "dir/",
        "unicodé.txt",
        "with space+plus.txt",
        "z/",
    ], paged
    expect_error(lambda: s3.list_objects(Bucket="missing-bucket"), 404)
    print("paged through objects with ListObjects, with and without a delimiter")


def check_delete_object(s3, data_root: Path):
    """Check DeleteObject with missing keys and buckets, and empty folder clean-up."""
    response = s3.delete_object(Bucket=LIST_BUCKET, Key="dir/sub/three.txt")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
    expect_error(
        lambda: s3.get_object(Bucket=LIST_BUCKET, Key="dir/sub/three.txt"), 404
    )
    assert not (data_root / LIST_BUCKET / "dir" / "sub").exists(), (
        "empty folder was not removed"
    )
    assert (data_root / LIST_BUCKET / "dir" / "one.txt").is_file(), (
        "sibling file was removed"
    )
    keys = [
        item["Key"]
        for item in s3.list_objects_v2(Bucket=LIST_BUCKET, Prefix="dir/")["Contents"]
    ]
    assert keys == ["dir/one.txt", "dir/two.txt"], f"unexpected keys {keys}"
    print("deleted an object and removed its empty folder")

    response = s3.delete_object(Bucket=LIST_BUCKET, Key="does-not-exist.txt")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
    expect_error(lambda: s3.delete_object(Bucket="missing-bucket", Key="x.txt"), 404)
    print("deleting a missing key returns 204 and a missing bucket returns 404")


def read_body(s3, bucket: str, key: str) -> bytes:
    """Return an object's content."""
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read()


def check_unsupported_query(s3):
    """Check requests for tagging, ACLs and the like are rejected and change nothing."""
    tagging = {"TagSet": [{"Key": "colour", "Value": "blue"}]}
    expect_error(
        lambda: s3.put_object_tagging(Bucket=BUCKET, Key=KEY, Tagging=tagging),
        501,
        "NotImplemented",
    )
    expect_error(
        lambda: s3.get_object_acl(Bucket=BUCKET, Key=KEY), 501, "NotImplemented"
    )
    assert read_body(s3, BUCKET, KEY) == CONTENT, (
        "unsupported request changed the object"
    )
    print("unsupported query parameters return 501 and leave the object unchanged")

    assert s3.get_object_tagging(Bucket=BUCKET, Key=KEY)["TagSet"] == [], (
        "tag set is not empty"
    )
    expect_error(
        lambda: s3.get_object_tagging(Bucket=BUCKET, Key="missing.txt"),
        404,
        "NoSuchKey",
    )
    print("reading tags returns an empty tag set")


def check_range_requests(s3):
    """Check GetObject with plain, open-ended, suffix and unsatisfiable ranges."""
    data = b"0123456789" * 10
    s3.put_object(Bucket=TRANSFER_BUCKET, Key="digits.txt", Body=data)
    cases = {
        "bytes=0-9": (0, 9),
        "bytes=90-": (90, 99),
        "bytes=-5": (95, 99),
        "bytes=95-200": (95, 99),
    }
    for header, (start, end) in cases.items():
        response = s3.get_object(Bucket=TRANSFER_BUCKET, Key="digits.txt", Range=header)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 206, (
            f"{header} did not return 206"
        )
        assert response["ContentRange"] == f"bytes {start}-{end}/100", (
            f"{header}: {response['ContentRange']}"
        )
        assert response["Body"].read() == data[start : end + 1], (
            f"{header} returned the wrong bytes"
        )
    full = s3.get_object(Bucket=TRANSFER_BUCKET, Key="digits.txt")
    assert full["AcceptRanges"] == "bytes" and full["Body"].read() == data
    expect_error(
        lambda: s3.get_object(
            Bucket=TRANSFER_BUCKET, Key="digits.txt", Range="bytes=100-"
        ),
        416,
        "InvalidRange",
    )
    print("range requests return the requested bytes")


def raw_request(
    port: int, method: str, path: str, headers: dict[str, str]
) -> http.client.HTTPResponse:
    """Send a plain HTTP request to the server and return the read response."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    try:
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        response.read()
        return response
    finally:
        connection.close()


def check_cors(port: int):
    """Check CORS preflights are allowed and responses carry CORS headers."""
    origin = "http://localhost:8000"
    response = raw_request(
        port,
        "OPTIONS",
        f"/{BUCKET}/{KEY}",
        {
            "Origin": origin,
            "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type,x-amz-date",
        },
    )
    assert response.status == 200, f"preflight returned {response.status}"
    assert response.getheader("Access-Control-Allow-Origin") == origin
    assert "PUT" in response.getheader("Access-Control-Allow-Methods", "")
    assert (
        response.getheader("Access-Control-Allow-Headers") == "content-type,x-amz-date"
    )
    print("CORS preflight requests are allowed")

    response = raw_request(port, "GET", f"/{BUCKET}/{KEY}", {"Origin": origin})
    assert response.status == 200
    assert response.getheader("Access-Control-Allow-Origin") == origin
    assert "ETag" in response.getheader("Access-Control-Expose-Headers", "")
    response = raw_request(port, "GET", f"/{BUCKET}/missing.txt", {"Origin": origin})
    assert response.status == 404
    assert response.getheader("Access-Control-Allow-Origin") == origin
    response = raw_request(port, "GET", f"/{BUCKET}/{KEY}", {})
    assert response.getheader("Access-Control-Allow-Origin") is None
    print("responses and errors carry CORS headers only for requests with an Origin")


def check_response_overrides(s3, port: int):
    """Check response-* query parameters override GetObject and HeadObject headers."""
    overrides = {
        "ResponseCacheControl": "no-cache",
        "ResponseContentDisposition": 'attachment; filename="report.txt"',
        "ResponseContentEncoding": "identity",
        "ResponseContentLanguage": "en",
        "ResponseContentType": "application/x-custom",
        "ResponseExpires": "Thu, 01 Jan 2099 00:00:00 GMT",
    }
    response = s3.get_object(Bucket=BUCKET, Key=KEY, **overrides)
    headers = response["ResponseMetadata"]["HTTPHeaders"]
    assert response["Body"].read() == CONTENT
    assert headers["cache-control"] == "no-cache"
    assert headers["content-disposition"] == 'attachment; filename="report.txt"'
    assert headers["content-encoding"] == "identity"
    assert headers["content-language"] == "en"
    assert headers["content-type"] == "application/x-custom"
    assert headers["expires"] == "Thu, 01 Jan 2099 00:00:00 GMT"
    response = s3.head_object(
        Bucket=BUCKET, Key=KEY, ResponseContentType="application/x-custom"
    )
    assert response["ContentType"] == "application/x-custom"
    assert s3.head_object(Bucket=BUCKET, Key=KEY)["ContentType"] == "text/plain"
    print("response-* parameters override GetObject and HeadObject headers")

    url = s3.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": BUCKET,
            "Key": KEY,
            "ResponseContentDisposition": "attachment",
        },
    )
    response = raw_request(port, "GET", url.split(str(port), 1)[1], {})
    assert response.status == 200
    assert response.getheader("Content-Disposition") == "attachment"
    response = raw_request(
        port,
        "GET",
        f"/{BUCKET}/{KEY}?response-content-type=text/plain%0D%0AX-Injected:%201",
        {},
    )
    assert response.status == 400, f"header injection returned {response.status}"
    assert response.getheader("X-Injected") is None
    print("presigned URLs accept overrides, and control characters are rejected")


def check_copy_object(s3):
    """Check CopyObject within and across buckets, onto itself and from missing keys."""
    source = {"Bucket": BUCKET, "Key": KEY}
    response = s3.copy_object(Bucket=BUCKET, Key="copies/hello.txt", CopySource=source)
    original_etag = s3.head_object(Bucket=BUCKET, Key=KEY)["ETag"]
    assert response["CopyObjectResult"]["ETag"] == original_etag, (
        "copy has a different ETag"
    )
    assert read_body(s3, BUCKET, "copies/hello.txt") == CONTENT, (
        "copy within a bucket has the wrong content"
    )
    s3.copy_object(Bucket=TRANSFER_BUCKET, Key="hello-copy.txt", CopySource=source)
    assert read_body(s3, TRANSFER_BUCKET, "hello-copy.txt") == CONTENT, (
        "copy across buckets has the wrong content"
    )
    print("copied objects within and across buckets")

    expect_error(
        lambda: s3.copy_object(Bucket=BUCKET, Key=KEY, CopySource=source),
        400,
        "InvalidRequest",
    )
    s3.copy_object(
        Bucket=BUCKET, Key=KEY, CopySource=source, MetadataDirective="REPLACE"
    )
    assert read_body(s3, BUCKET, KEY) == CONTENT, "copy onto itself changed the content"
    missing = {"Bucket": BUCKET, "Key": "missing.txt"}
    expect_error(
        lambda: s3.copy_object(Bucket=BUCKET, Key="x.txt", CopySource=missing),
        404,
        "NoSuchKey",
    )
    print("copy onto itself needs REPLACE, and a missing source returns 404")

    s3.copy_object(
        Bucket=BUCKET,
        Key="copies/if-match.txt",
        CopySource=source,
        CopySourceIfMatch=original_etag,
    )
    assert read_body(s3, BUCKET, "copies/if-match.txt") == CONTENT, (
        "conditional copy has the wrong content"
    )
    expect_error(
        lambda: s3.copy_object(
            Bucket=BUCKET,
            Key="copies/no.txt",
            CopySource=source,
            CopySourceIfMatch='"abc"',
        ),
        412,
        "PreconditionFailed",
    )
    expect_error(
        lambda: s3.copy_object(
            Bucket=BUCKET,
            Key="copies/no.txt",
            CopySource=source,
            CopySourceIfNoneMatch=original_etag,
        ),
        412,
        "PreconditionFailed",
    )
    expect_error(lambda: s3.head_object(Bucket=BUCKET, Key="copies/no.txt"), 404)
    print("copy conditions are honoured")


def check_multipart_upload(s3, data_root: Path):
    """Check the multipart upload operations directly, including their error cases."""
    key = "multipart/joined.bin"
    first, second = os.urandom(PART_SIZE), os.urandom(1000)
    upload_id = s3.create_multipart_upload(Bucket=TRANSFER_BUCKET, Key=key)["UploadId"]
    etags = [
        s3.upload_part(
            Bucket=TRANSFER_BUCKET,
            Key=key,
            UploadId=upload_id,
            PartNumber=number,
            Body=body,
        )["ETag"]
        for number, body in ((1, first), (2, second))
    ]
    listed = s3.list_objects_v2(Bucket=TRANSFER_BUCKET, Prefix="multipart/")
    assert "Contents" not in listed, "an unfinished upload appeared in the listing"
    expect_error(lambda: s3.head_object(Bucket=TRANSFER_BUCKET, Key=key), 404)

    def complete(parts, upload=upload_id, target=key):
        """Call CompleteMultipartUpload with the given (part number, ETag) pairs."""
        listing = {
            "Parts": [{"PartNumber": number, "ETag": etag} for number, etag in parts]
        }
        return s3.complete_multipart_upload(
            Bucket=TRANSFER_BUCKET, Key=target, UploadId=upload, MultipartUpload=listing
        )

    expect_error(
        lambda: complete([(2, etags[1]), (1, etags[0])]), 400, "InvalidPartOrder"
    )
    expect_error(lambda: complete([(1, etags[1]), (2, etags[1])]), 400, "InvalidPart")
    expect_error(lambda: complete([(1, etags[0]), (3, etags[1])]), 400, "InvalidPart")
    expect_error(
        lambda: complete([(1, etags[0])], target="multipart/other.bin"),
        404,
        "NoSuchUpload",
    )
    print(
        "multipart completion rejects wrong order, wrong ETags, missing parts "
        "and the wrong key"
    )

    complete([(1, etags[0]), (2, etags[1])])
    assert read_body(s3, TRANSFER_BUCKET, key) == first + second, (
        "joined object has the wrong content"
    )
    assert (data_root / TRANSFER_BUCKET / key).read_bytes() == first + second, (
        "joined file on disk is wrong"
    )
    assert not any((data_root / ".localbucket-uploads").iterdir()), (
        "completed upload was not cleaned up"
    )
    expect_error(lambda: complete([(1, etags[0]), (2, etags[1])]), 404, "NoSuchUpload")
    print("completed a multipart upload and removed its parts")

    small_id = s3.create_multipart_upload(
        Bucket=TRANSFER_BUCKET, Key="multipart/small.bin"
    )["UploadId"]
    small = [
        s3.upload_part(
            Bucket=TRANSFER_BUCKET,
            Key="multipart/small.bin",
            UploadId=small_id,
            PartNumber=n,
            Body=b"x",
        )["ETag"]
        for n in (1, 2)
    ]
    expect_error(
        lambda: complete(
            [(1, small[0]), (2, small[1])],
            upload=small_id,
            target="multipart/small.bin",
        ),
        400,
        "EntityTooSmall",
    )
    response = s3.abort_multipart_upload(
        Bucket=TRANSFER_BUCKET, Key="multipart/small.bin", UploadId=small_id
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
    expect_error(
        lambda: s3.upload_part(
            Bucket=TRANSFER_BUCKET,
            Key="multipart/small.bin",
            UploadId=small_id,
            PartNumber=3,
            Body=b"x",
        ),
        404,
        "NoSuchUpload",
    )
    expect_error(
        lambda: s3.head_object(Bucket=TRANSFER_BUCKET, Key="multipart/small.bin"), 404
    )
    print("small parts are rejected and aborted uploads are gone")


def check_managed_transfers(s3):
    """Check boto3's managed transfers use multipart uploads, ranges and part copies."""
    data = os.urandom(PART_SIZE * 2 + 12345)
    config = TransferConfig(
        multipart_threshold=PART_SIZE, multipart_chunksize=PART_SIZE
    )
    counts = {"UploadPart": 0, "UploadPartCopy": 0, "Range": 0}

    def count(model, params, **_):
        """Count the requests that the managed transfers send."""
        if model.name in counts:
            counts[model.name] += 1
        if model.name == "GetObject" and "Range" in params.get("headers", {}):
            counts["Range"] += 1

    s3.meta.events.register("before-call.s3", count, unique_id="count-transfers")
    try:
        s3.upload_fileobj(
            io.BytesIO(data), TRANSFER_BUCKET, "managed/big.bin", Config=config
        )
        downloaded = io.BytesIO()
        s3.download_fileobj(
            TRANSFER_BUCKET, "managed/big.bin", downloaded, Config=config
        )
        source = {"Bucket": TRANSFER_BUCKET, "Key": "managed/big.bin"}
        s3.copy(source, BUCKET, "managed/big-copy.bin", Config=config)
    finally:
        s3.meta.events.unregister("before-call.s3", unique_id="count-transfers")

    assert downloaded.getvalue() == data, "managed download returned the wrong content"
    assert read_body(s3, BUCKET, "managed/big-copy.bin") == data, (
        "managed copy has the wrong content"
    )
    assert counts["UploadPart"] == 3, (
        f"expected 3 uploaded parts, got {counts['UploadPart']}"
    )
    assert counts["UploadPartCopy"] == 3, (
        f"expected 3 copied parts, got {counts['UploadPartCopy']}"
    )
    assert counts["Range"] >= 2, f"expected ranged downloads, got {counts['Range']}"
    print(
        f"managed transfers used {counts['UploadPart']} parts, "
        f"{counts['UploadPartCopy']} part copies "
        f"and {counts['Range']} ranged reads"
    )


def check_delete_bucket(s3, data_root: Path):
    """Check DeleteBucket refuses non-empty buckets and removes the bucket's uploads."""
    s3.create_bucket(Bucket=DELETE_BUCKET)
    s3.put_object(Bucket=DELETE_BUCKET, Key="dir/kept.txt", Body=b"kept")
    expect_error(lambda: s3.delete_bucket(Bucket=DELETE_BUCKET), 409, "BucketNotEmpty")
    assert read_body(s3, DELETE_BUCKET, "dir/kept.txt") == b"kept", (
        "refused delete changed the bucket"
    )
    print("deleting a non-empty bucket returns 409 BucketNotEmpty")

    upload_id = s3.create_multipart_upload(Bucket=DELETE_BUCKET, Key="big.bin")[
        "UploadId"
    ]
    other_id = s3.create_multipart_upload(Bucket=BUCKET, Key="other.bin")["UploadId"]
    s3.delete_object(Bucket=DELETE_BUCKET, Key="dir/kept.txt")
    response = s3.delete_bucket(Bucket=DELETE_BUCKET)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204
    assert not (data_root / DELETE_BUCKET).exists(), "bucket directory was not removed"
    names = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
    assert DELETE_BUCKET not in names, f"deleted bucket is still listed in {names}"
    expect_error(lambda: s3.head_bucket(Bucket=DELETE_BUCKET), 404)
    uploads = data_root / ".localbucket-uploads"
    assert not (uploads / upload_id).exists(), "the bucket's upload was not removed"
    assert (uploads / other_id).is_dir(), "another bucket's upload was removed"
    print("deleted an empty bucket and its unfinished uploads")

    s3.create_bucket(Bucket=DELETE_BUCKET)
    expect_error(
        lambda: s3.upload_part(
            Bucket=DELETE_BUCKET,
            Key="big.bin",
            UploadId=upload_id,
            PartNumber=1,
            Body=b"x",
        ),
        404,
        "NoSuchUpload",
    )
    s3.delete_bucket(Bucket=DELETE_BUCKET)
    s3.abort_multipart_upload(Bucket=BUCKET, Key="other.bin", UploadId=other_id)
    expect_error(
        lambda: s3.delete_bucket(Bucket=DELETE_BUCKET), 404, "NoSuchBucket"
    )
    print("a recreated bucket has no old uploads, and missing buckets return 404")


def main():
    """Run all checks against a fresh localbucket server."""
    with tempfile.TemporaryDirectory() as data_root:
        port = free_port()
        server = subprocess.Popen(
            [sys.executable, LOCALBUCKET, data_root, "--port", str(port)]
        )
        try:
            wait_for_port(port)
            s3 = boto3.client(
                "s3",
                endpoint_url=f"http://127.0.0.1:{port}",
                aws_access_key_id="test",
                aws_secret_access_key="test",
                region_name="us-east-1",
                config=Config(s3={"addressing_style": "path"}),
            )

            s3.create_bucket(Bucket=BUCKET)
            assert (Path(data_root) / BUCKET).is_dir(), (
                "bucket directory was not created"
            )
            print(f"created bucket {BUCKET}")

            s3.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
            assert (Path(data_root) / BUCKET / KEY).read_bytes() == CONTENT, (
                "file on disk does not match"
            )
            print(f"wrote {KEY}")

            body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
            assert body == CONTENT, f"read back {body!r}, expected {CONTENT!r}"
            print(f"read {KEY}: {body!r}")

            expect_error(
                lambda: s3.put_object(
                    Bucket=BUCKET, Key=KEY, Body=b"new", IfNoneMatch="*"
                ),
                412,
            )
            assert s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read() == CONTENT, (
                "conditional put overwrote"
            )
            s3.put_object(
                Bucket=BUCKET, Key="fresh.txt", Body=b"fresh", IfNoneMatch="*"
            )
            print("conditional put returns 412 on an existing key")

            s3.create_bucket(Bucket=LIST_BUCKET)
            names = [bucket["Name"] for bucket in s3.list_buckets()["Buckets"]]
            assert names == sorted([BUCKET, LIST_BUCKET]), f"unexpected buckets {names}"
            print(f"listed buckets {names}")

            assert (
                s3.head_bucket(Bucket=BUCKET)["ResponseMetadata"]["HTTPStatusCode"]
                == 200
            )
            expect_error(lambda: s3.head_bucket(Bucket="missing-bucket"), 404)
            print("head bucket finds existing buckets and rejects missing ones")

            check_list_objects(s3)
            check_list_objects_v1(s3)
            check_delete_object(s3, Path(data_root))
            s3.create_bucket(Bucket=TRANSFER_BUCKET)
            check_unsupported_query(s3)
            check_range_requests(s3)
            check_cors(port)
            check_response_overrides(s3, port)
            check_copy_object(s3)
            check_multipart_upload(s3, Path(data_root))
            check_managed_transfers(s3)
            check_delete_bucket(s3, Path(data_root))

            print("all checks passed")
        finally:
            server.terminate()
            server.wait()


if __name__ == "__main__":
    main()
