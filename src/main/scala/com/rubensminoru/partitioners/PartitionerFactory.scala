package com.rubensminoru.partitioners;

object PartitionerFactory {
    def apply() = new TimeBasedPartitioner()
}
