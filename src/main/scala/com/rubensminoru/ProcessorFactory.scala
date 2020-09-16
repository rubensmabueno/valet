package com.rubensminoru;

import com.rubensminoru.partitioners.Partitioner

object ProcessorFactory {
    def apply(topic:String, path:String, partitioner:Partitioner):Processor = new Processor(topic, path, partitioner)
}
