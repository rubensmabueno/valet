package com.rubensminoru.main;

import com.rubensminoru.messages.KafkaMessage;
import com.rubensminoru.partitioners.Partitioner;
import com.rubensminoru.writers.Writer;
import com.rubensminoru.writers.WriterFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Processor {
    private String topic;
    private final WriterFactory writerFactory;
    private final Partitioner partitioner;

    private Map<Writer, List<TopicInfo>> writerTopicInfos;
    private List<TopicInfo> topicInfos;

    public Processor(String topic, WriterFactory writerFactory, Partitioner partitioner) {
        this.topic = topic;
        this.writerFactory = writerFactory;
        this.partitioner = partitioner;
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

    public Map<Writer, List<TopicInfo>> addOrUpdateWriter(Map<Writer, List<TopicInfo>> localWriterTopicInfos, KafkaMessage message) {
        Writer currentWriter = null;
        List<TopicInfo> localTopicInfos = new ArrayList<>();

        for (Writer writer : localWriterTopicInfos.keySet()) {
            if (checkRecordPartition(message, writer)) {
                currentWriter = writer;
            }
        }

        if (currentWriter == null) {
            currentWriter = this.writerFactory.createInstance();
        } else {
            localTopicInfos = localWriterTopicInfos.get(currentWriter);
        }

        localTopicInfos = this.addOrUpdatePartitionInfo(localTopicInfos, message);

        currentWriter.write(message);

        localWriterTopicInfos.put(currentWriter, localTopicInfos);

        return localWriterTopicInfos;
    }

    public List<TopicInfo> addOrUpdatePartitionInfo(List<TopicInfo> localTopicInfos, KafkaMessage message) {
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

    public boolean checkRecordPartition(KafkaMessage message, Writer writer) {
        return partitioner.check(message, writer);
    }

    public static class TopicInfo {
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

        public String getTopic() { return topic; }

        public long getOffset() { return offset; }
    }
}
