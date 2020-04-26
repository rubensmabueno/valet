package com.rubensminoru.writers;

public class WriterFactory {
    public Writer createInstance() {
        return new ParquetWriter();
    }
}
