package com.rubensminoru.writers;

import com.rubensminoru.messages.KafkaMessage;

public interface Writer {
    void write(KafkaMessage message);

    long getTimestamp();
}
