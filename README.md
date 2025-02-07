
This is a fork of https://github.com/jortage/poolmgr

:warning: Currently broken, in the process of adding DB migration and switching to Postgres. :warning:
\
For a working docker compose to test the proxy see: https://github.com/Timshel/jortage-poolmgr

## Usage

The `--build` ensure that the image is up to date (In case of `Could not connect to 127.0.0.1:3306` error just retry, the DB was just not completely up).

```bash
> docker compose up --build S3DedupProxy
```

This will run three services:

- `S3DedupProxy`: the proxy itself (bind 23278, 23279 and 23290)
- `Postgres`: the database to store the file metadata (binded on 3306)
- `S3Proxy`: used as local S3 store (binded on 8080)

You will need to create the `blobs` bucket in the `S3Proxy` store, since it's run without authentication it can be done with `curl`:

```bash
curl --request PUT http://127.0.0.1:8080/blobs
```

You can then interact with the proxy and store using [MinIO client](https://min.io/docs/minio/linux/reference/minio-mc.html).

```config
{
    "version": "10",
    "aliases": {
        "proxy": {
            "url": "http://127.0.0.1:23278",
            "accessKey": "test",
            "secretKey": "ff54fae017ebe20356223ea016d1e2e867b7f73aaba775fe48ed65d1f1fecb87",
            "api": "S3v4",
            "path": "auto"
        },
        "store": {
            "url": "http://127.0.0.1:8080",
            "api": "S3v4",
            "path": "auto"
        },
    }
}
```

By default the `test` user must write in the `test` bucket, ex:

```bash
> mc ls store/blobs/
> mc cp dark.png jortage/test/
> mc ls store/blobs/
[2024-11-21 19:21:25 CET]     0B a/
```

Have fun

## Remarks

On the strange config you might find:

- `s3.lan`: some part of the code require a domain name and not a host.
- `mastodon.s3.lan`: not sure why but the `bucket` from the config endup as the subdomain of the called url
