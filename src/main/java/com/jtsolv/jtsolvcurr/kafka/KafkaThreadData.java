package com.jtsolv.jtsolvcurr.kafka;

import java.util.Map;
import java.util.TreeMap;

public class KafkaThreadData {

    private Map<String, String> partitions = new TreeMap();

    public Map<String, String> getPartitions() {
        return partitions;
    }

    public void setPartitions(Map<String, String> partitions) {
        this.partitions = partitions;
    }

    public void addPartition(String partition){
        partitions.putIfAbsent(partition,partition);
    }

}
