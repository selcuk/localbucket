# LocalBucket

LocalBucket is a minimal S3-compatible HTTP server that stores buckets as
folders on a local directory. It is a single Python script for Python 3.10
and newer, with no dependencies beyond the standard library, meant for
local development and testing against S3 clients such as boto3 and the AWS
CLI.

Each bucket is a folder under the data directory and each object is a
regular file, so you can inspect and change the stored data with ordinary
file tools.

## Installation

```sh
uv tool install localbucket   # or: pipx install localbucket
```

You can also run it without installing, with `uvx localbucket ./data`, or
copy `localbucket.py` anywhere and run it with `python3 localbucket.py`.

## Usage

```sh
localbucket ./data
```

This serves `./data` on `http://127.0.0.1:9000`. Use `--host` and `--port`
to change the address.

Point a client at the server with path-style addressing. Credentials are not
checked, so any access key and secret will do:

```sh
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://mybucket
aws --endpoint-url http://127.0.0.1:9000 s3 cp file.txt s3://mybucket/
```

```python
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)
```

## Supported operations

- Buckets: ListBuckets, CreateBucket, HeadBucket, and DeleteBucket (empty
  buckets only)
- Objects: ListObjects, ListObjectsV2, PutObject (including
  `If-None-Match: *`), GetObject and HeadObject (including `Range` and
  `response-*` header overrides such as `response-content-disposition`),
  CopyObject, DeleteObject
- Multipart uploads: CreateMultipartUpload, UploadPart, UploadPartCopy,
  CompleteMultipartUpload, AbortMultipartUpload
- GetObjectTagging, which always returns an empty tag set because tags are
  not stored

Requests with any other query parameter, such as ACL or tagging changes,
return `501 NotImplemented` and do not change anything. Virtual-hosted-style
URLs, authentication and versioning are not supported. Object metadata is
not stored either: `Content-Type` is guessed from the key's file extension.

Unfinished multipart uploads are kept in `DATA_ROOT/.localbucket-uploads`.

CORS is open: requests from any origin are allowed, including `OPTIONS`
preflights, so browser code on your development site can call LocalBucket
directly. Any web page you open can also read from it while it runs, so
keep it on `127.0.0.1` and don't store anything sensitive in it.

## Testing

The test suite starts LocalBucket on a temporary directory and checks its
behaviour with boto3. It is a [uv](https://docs.astral.sh/uv/) script that
declares its own dependencies:

```sh
uv run test_localbucket.py
```
