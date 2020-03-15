package com.rubensminoru.partitioners;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.writers.ParquetWriter;

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

    public boolean process(List<KafkaMessage> messages) {
        for (KafkaMessage message : messages) {
            process(message);
        }

        return true;
    }

    public void process(KafkaMessage message) {
        this.writerTopicInfos = this.addOrUpdateWriter(this.writerTopicInfos, message);

        this.topicInfos = this.addOrUpdatePartitionInfo(this.topicInfos, message);
    }

    public boolean checkRecordPartition(long recordTimestamp, long checkTimestamp) {
        return ((int) (recordTimestamp / DURATION_MS)) == ((int) (checkTimestamp / DURATION_MS));
    }

    private Map<ParquetWriter, List<TopicInfo>> addOrUpdateWriter(Map<ParquetWriter, List<TopicInfo>> localWriterTopicInfos, KafkaMessage message) {
        ParquetWriter parquetWriter = null;
        List<TopicInfo> localTopicInfos = new ArrayList<>();

        for (ParquetWriter writer : localWriterTopicInfos.keySet()) {
            if (checkRecordPartition(message.getTimestamp(), writer.getTimestamp())) {
                parquetWriter = writer;
            }
        }

        if (parquetWriter == null) {
            parquetWriter = new ParquetWriter();
        } else {
            localTopicInfos = localWriterTopicInfos.get(parquetWriter);
        }

        localTopicInfos = this.addOrUpdatePartitionInfo(localTopicInfos, message);

        parquetWriter.write(message);

        localWriterTopicInfos.put(parquetWriter, localTopicInfos);

        return localWriterTopicInfos;
    }

    private List<TopicInfo> addOrUpdatePartitionInfo(List<TopicInfo> localTopicInfos, KafkaMessage message) {
        Integer partitionIndex = null;
        TopicInfo topicInfo;

        for (TopicInfo localTopicInfo : localTopicInfos) {
            if (localTopicInfo.getPartition() == message.getPartition()) {
                partitionIndex = localTopicInfos.indexOf(localTopicInfo);
            }
        }

        if (partitionIndex == null) {
            topicInfo = new TopicInfo(this.topic, message.getPartition(), message.getOffset());
            localTopicInfos.add(topicInfo);
        } else {
            topicInfo = localTopicInfos.get(partitionIndex);

            topicInfo.setOffset(message.getOffset());

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
