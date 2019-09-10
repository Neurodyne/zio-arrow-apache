# zio-serdes
Serialization and Deserialization library for [ZIO](https://zio.dev/), [Apache Arrow](https://arrow.apache.org/), [Apache Parquet](http://parquet.apache.org/), Java NIO and Java NIO2

## Definitions
**Serialization** is the process of transforming complex data structures into a raw array of bytes. This lib converts supported formats into Array[Byte], which is the lowest level of data representation.

Array[Byte] is widely supported by hardware and communication protocols. This allows to seamlessly transmit data over the network, read/write onto disk, store temporary in memory, cache, allow RAID support and other data formats.

**Deserialization** is the process of converting raw bytes into meaningful objects, which can be used in programming environments. For example, deserializing Array[Byte] into ZIO Chunk yields in the zio.Chunk[A] object on output.

## Supported formats

* **ZIO Chunk**: Chunk[A] <---> Array[Byte]
* **Apache Arrow**: Arrow [ Chunk[Int] ] <---> Array[Byte]

## Coming soon
* Support for Apache Parquet
* Support for NIO/NIO2
