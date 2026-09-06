package com.jtsolv.jtsolvcurr.kafka;

import io.micrometer.common.util.StringUtils;

import java.util.Map;
import java.util.TreeMap;

public class KafkaPartitionData {

    private String threadId;
    private String partitionId;
    private int messagesCount = 0;

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    private Map<String, String> offsets = new TreeMap();

    public Map<String, String> getOffsets() {
        return offsets;
    }

    public void setOffsets(Map<String, String> offsets) {
        this.offsets = offsets;
    }

    public int getMessagesCount() {
        return messagesCount;
    }

    public void setMessagesCount(int messagesCount) {
        this.messagesCount = messagesCount;
    }


    public String getThreadId() {
        return threadId;
    }

    public void setThreadId(String threadId) {
        this.threadId = threadId;
    }


    public void addOffset(String offset){
        if(StringUtils.isEmpty(offset)){
            return;
        }
        offsets.putIfAbsent(offset, offset);
    }

}
