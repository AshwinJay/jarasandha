[![Build Status](https://travis-ci.org/AshwinJay/jarasandha.svg?branch=master)](https://travis-ci.org/AshwinJay/jarasandha)  [![Code Coverage](https://codecov.io/gh/AshwinJay/jarasandha/branch/master/graph/badge.svg)](https://codecov.io/gh/AshwinJay/jarasandha) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/c46c16421cb04033b0439eb385917bd2)](https://www.codacy.com/app/AshwinJay/jarasandha?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=AshwinJay/jarasandha&amp;utm_campaign=Badge_Grade)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.ashwinjay/jarasandha-store-filesystem.svg)](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22jarasandha-store-filesystem%22) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/AshwinJay/jarasandha/blob/master/LICENSE)

----

# Table Of Contents

<!-- ts -->
<!-- cat README.md | ~/dump/gh-md-toc/gh-md-toc (From https://github.com/ekalinin/github-markdown-toc) -->
  * [Introduction](#introduction)
    * [What is it?](#what-is-it)
        * [File](#file)
        * [Writer](#writer)
        * [Reader](#reader)
    * [What it is not](#what-it-is-not)
    * [What's with the name?](#whats-with-the-name)
    * [License](#license)
  * [Possible use cases](#possible-use-cases)
    * [Hot-cold store](#hot-cold-store)
  * [Basics](#basics)
  * [Advanced](#advanced)
  * [Architecture](#architecture)
<!-- te -->

# Introduction

### What is it?
Jarasandha is a small Java (version 8+) library to help build an archive of records. It has very few moving parts, embraces immutability and provides efficient compression, buffer management and zero copy transfer. It delegates advanced functions to external services using interfaces.

It is composed of these parts:

#### File
1. A file format that has blocks, records and an index 
1. Blocks can be compressed (optional). Blocks contain records
1. The file is immutable, meaning once the file with all its records is written it cannot be modified
1. The file is a "write once and read many times" format
1. Checksums and compression on the internal index and blocks

#### Writer
1. Records are written one at a time to the file using a "writer". The writer returns a logical position within the file that has to be stored in an external system
1. Internally, of course the records are flushed to the file one block at a time
1. The "writer" and related classes provide ways to manage collections of files and hooks to archive to external stores

#### Reader
1. Records can be retrieved using a "reader" by providing its logical position
1. It also supports iterating over the records or blocks of records in the file
1. The "reader" and related classes provide efficient, selective loading and caching of blocks and files for repeated reads
1. It also has hooks to read from external stores
1. It is meant to be embedded inside your application that serves records from a remote archive and a local file system
1. Both the reader and writer components make heavy use of Netty's [Bytebuf](http://netty.io/4.0/api/index.html?io/netty/buffer/ByteBuf.html) to keep heap and in general memory usage low with a controllable budget

### What it is not
Jarasandha does not aim to compete with systems or libraries like [Apache ORC](https://orc.apache.org/) or [Apache Parquet](https://parquet.apache.org/) or [PalDB](https://github.com/linkedin/PalDB) or [embedded Key-Value stores](https://github.com/lmdbjava/benchmarks) or [Ambry](https://github.com/linkedin/ambry/wiki) or [Apache HBase](https://hbase.apache.org/).

1. It does not provide key-value access, rather it provides a simple position based access to records
2. It is not a database of any sort
3. It has no opinion in terms of what you store as a record but it can compress a block that has multiple records before storing them to the file
4. It does not provide querying or searching based on keys or values rather on logical positions

### What's with the name?
The name (`Jarasandha`) is a reference to an [Indian mythological character named Jarasandha](https://en.wikipedia.org/wiki/Jarasandha) who was put back together from two halves. I found the name vaguely related to this Java library which puts your records back together from blocks of compressed records in a file. Well, I did say - "vaguely related".

### License
The Jarasandha library is licensed under the [Apache License](LICENSE).

# Possible use cases

### Hot-cold store

Store records in Jarasandha, move the files out to object stores like [Amazon S3](https://aws.amazon.com/s3/) or [Minio](https://minio.io/) when they are not in use.

Jarasandha can be the underlying layer that efficiently stores and retrieves records and blocks based on logical key positions. A second index layer using Lucene or RocksDB could provide a more advanced mapping from keys, labels or queries to Jarasandha's logical key positions.

Assuming that the keys and metadata to service queries are much smaller than the actual records, they can be stored onsite, on fast and expensive hardware. The actual record can then be retrieved from the Jarasandha files and blocks that are cached locally or downloaded on demand from remote object stores.

See [Hot-cold store](doc/hot-cold-store.md) for details.

# Basics
[ReadersAndWritersDemoTest](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store-filesystem/src/test/java/io/jarasandha/store/filesystem/ReadersAndWritersDemoTest.java) demonstrates how to write to files and also to read them back using the APIs:

* [StoreReader](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store/src/main/java/io/jarasandha/store/api/StoreReader.java)
* [StoreWriter](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store/src/main/java/io/jarasandha/store/api/StoreWriter.java)
* [Index](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store/src/main/java/io/jarasandha/store/api/Index.java)
* [Block](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store/src/main/java/io/jarasandha/store/api/BlockWithRecordOffsets.java)

----

⚠️ The following content is work in progress ⚠️

----

# Advanced

A [command line tool](https://github.com/AshwinJay/jarasandha/blob/master/jarasandha-store-filesystem/src/main/java/io/jarasandha/store/filesystem/cli) to import and inspect encoded files is also available.

Efficiency

Compression, blocks, memory efficiency of ByteBuf, native heap size.

Zero copy with compressed index and uncompressed blocks. Zip the entire file while archiving.

Writing - NoOpFileWriteProgressListener to push files to S3

Reading - DefaultFileEventListener to build archiving and retrieval

# Architecture

File format

Index and block format

Logical record position, need to secondary store

Compression and caching

Writer and reader efficiency - ButeBuf
