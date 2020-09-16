package com.rubensminoru.writers;

import com.rubensminoru.messages.KafkaAvroMessage
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths;

class ParquetWriter(schema:Schema, path:String) extends Writer {
    var timestamp:Long = 0L
    var filename:String = ""
    var writer:org.apache.parquet.hadoop.ParquetWriter[GenericRecord] = null

    def BLOCK_SIZE: Int = 256 * 1024 * 1024
    def PAGE_SIZE: Int = 64 * 1024

    def isSchemaCompatible(schema:Schema): Boolean = this.schema == schema

    @throws(classOf[Exception])
    def initializeWriter(message:KafkaAvroMessage): org.apache.parquet.hadoop.ParquetWriter[GenericRecord] = {
        this.timestamp = message.timestamp
        this.filename = message.partition + '_' + message.offset + ".parquet"

        if(Files.exists(Paths.get(this.path + filename)))
            throw new IOException("File already exists: " + this.path + filename)

        AvroParquetWriter.builder(new Path(this.path + filename))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(BLOCK_SIZE)
                .withPageSize(PAGE_SIZE)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()
    }

    @throws(classOf[Exception])
    def write(message:KafkaAvroMessage) = {
        if(this.writer == null) {
            this.writer = this.initializeWriter(message)
        }

        this.writer.write(message.value)
    }

    @throws(classOf[Exception])
    def close = {
        this.writer.close()
    }

    def getTimestamp: Long = {
        this.timestamp
    }
}
