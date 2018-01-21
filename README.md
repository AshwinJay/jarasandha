# Jarasandha

### `This document is a work in progress`

# What

### What's with the name?
The name (`Jarasandha`) is a reference to an [Indian mythological character named Jarasandha](https://en.wikipedia.org/wiki/Jarasandha) who was put back together from two halves. I found the name vaguely related to this Java library which puts your records back together from blocks of compressed records in a file. Well, I did say - "vaguely related".

# Basics
Read & Write

Efficiency

Example - based on Importer and FileReadersTest

Compression, blocks, memory efficiency ByteBuf

Pre-reqs: Java 8, Maven

# Example

FileWriters

FileReaders

Files

FileId

# Architecture

File format

Index and block format

Logical record position, need to secondary store

Compression and caching

Writer and reader efficiency - ButeBuf

# Misc

CLI importer

Writing - NoOpFileWriteProgressListener to push files to S3

Reading - DefaultFileEventListener to build archiving and retrieval

Avro vs ORC vs Parquet vs RDBMS vs [PalDB](https://github.com/linkedin/PalDB) vs [embedded KV stores](https://github.com/lmdbjava/benchmarks)
