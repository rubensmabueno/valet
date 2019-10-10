package com.rubensminoru.partitioner;

import com.rubensminoru.writer.ParquetWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeBasedPartitioner {
    private String topic;

    private Long DURATION_MS = 3600000L;

    private Map<ParquetWriter, List<TopicInfo>> writerTopicInfos;
    private List<TopicInfo> topicInfos;

    public TimeBasedPartitioner(String topic) {
        this.topic = topic;
        this.writerTopicInfos = new HashMap();
        this.topicInfos = new ArrayList<>();
    }

    public void process(ConsumerRecords<Long, GenericRecord> records) {
        for (ConsumerRecord<Long, GenericRecord> record : records) {
            process(record);
        }

        System.out.println(writerTopicInfos);
    }

    public void process(ConsumerRecord record) {
        this.writerTopicInfos = this.addOrUpdateWriter(this.writerTopicInfos, record);

        this.topicInfos = this.addOrUpdatePartitionInfo(this.topicInfos, record);
    }

    public boolean checkRecordPartition(long recordTimestamp, long checkTimestamp) {
        return ((int) (recordTimestamp / DURATION_MS)) == ((int) (checkTimestamp / DURATION_MS));
    }

    private Map<ParquetWriter, List<TopicInfo>> addOrUpdateWriter(Map<ParquetWriter, List<TopicInfo>> localWriterTopicInfos, ConsumerRecord record) {
        ParquetWriter parquetWriter = null;
        List<TopicInfo> localTopicInfos = new ArrayList<>();

        for (ParquetWriter writer : localWriterTopicInfos.keySet()) {
            if (checkRecordPartition(record.timestamp(), writer.getTimestamp())) {
                parquetWriter = writer;
            }
        }

        if (parquetWriter == null) {
            parquetWriter = new ParquetWriter();
        } else {
            localTopicInfos = localWriterTopicInfos.get(parquetWriter);
        }

        localTopicInfos = this.addOrUpdatePartitionInfo(localTopicInfos, record);

        parquetWriter.write(record);

        localWriterTopicInfos.put(parquetWriter, localTopicInfos);

        return localWriterTopicInfos;
    }

    private List<TopicInfo> addOrUpdatePartitionInfo(List<TopicInfo> localTopicInfos, ConsumerRecord record) {
        Integer partitionIndex = null;
        TopicInfo topicInfo;

        for (TopicInfo localTopicInfo : localTopicInfos) {
            if (localTopicInfo.getPartition() == record.partition()) {
                partitionIndex = localTopicInfos.indexOf(localTopicInfo);
            }
        }

        if (partitionIndex == null) {
            topicInfo = new TopicInfo(this.topic, record.partition(), record.offset());
            localTopicInfos.add(topicInfo);
        } else {
            topicInfo = localTopicInfos.get(partitionIndex);

            topicInfo.setOffset(record.offset());

            localTopicInfos.set(partitionIndex, topicInfo);
        }

        return localTopicInfos;
    }

    public class TopicInfo {
        private final String topic;
        private final int partition;
        private long offset;

        public TopicInfo(String topic, int partition, long offset) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
        }

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public int getPartition() {
            return partition;
        }
    }
}
