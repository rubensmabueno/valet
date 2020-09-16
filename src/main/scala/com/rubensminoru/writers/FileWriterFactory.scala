package com.rubensminoru.writers;

import org.apache.avro.Schema;

object FileWriterFactory {
    def apply(schema:Schema, path:String): Writer = new ParquetWriter(schema, path)
}
