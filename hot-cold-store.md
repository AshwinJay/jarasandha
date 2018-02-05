# Hot-cold archive

This page contains instructions to setup a userland [file system (FUSE)](https://en.wikipedia.org/wiki/Filesystem_in_Userspace) over an [S3](https://aws.amazon.com/s3/) compatible system. It could be AWS S3 itself or Minio which emulates the S3 interface. It assumes Mac OS.

## Why?

This gives you an idea to store your least frequently used files on S3 (or a remote Minio server) and then mount those locally using the FUSE so it looks like those files are available locally.

The more frequently used files could be in a different local directory.

This way you can move files about between the hot (local) & cold (remote - S3/Minio) directories. If the file is in the cold, mounted store it saves space locally but has higher latency when you access it.

## Minio

#### Install Minio to emulate "remote" S3 on your laptop

Install [https://minio.io/downloads.html#download-server-macos](https://minio.io/downloads.html#download-server-macos).

#### Setup data directory and buckets in Minio

The instructions are for running Minio and the userland filesystem on the same laptop but different directories. In reality it could be Minio on a set of servers and the FUSE on another machine. 

```
export DATA_DIR=~/dump/jarasandha.io/data

mkdir $DATA_DIR/remote
minio server $DATA_DIR/remote
```

Create buckets: `bucket-1, bucket-2 ...` and store your least frequently used files using the Minio UI.

## Goofys

#### Install Goofys to have a FUSE filesystem view of S3 (Minio)

Install [https://github.com/kahing/goofys](https://github.com/kahing/goofys).

Create `~/.aws/credentials` (if absent) and copy the credentials that Minio prints when it starts under a new `minio` profile.

```
[minio]
aws_access_key_id = <GET-FROM-MINIO-START-XXX>
aws_secret_access_key = <GET-FROM-MINIO-START-YYY>
```

#### Start Goofys with Minio as the endpoint, AWS profile name and the bucket name.

```
export DATA_DIR=~/dump/jarasandha.io/data

mkdir $DATA_DIR/mnt-bucket-1
goofys --profile minio -f --endpoint <GET-MINIO-ADDRESS> <MINIO-BUCKET-NAME> $DATA_DIR/mnt-bucket-1
umount -f $DATA_DIR/mnt-bucket-1
rm -rf $DATA_DIR/mnt-bucket-1
```

#### Debugging

* `pkill -9 goofys`
* `mount`