package com.jtsolv.jtsolvcurr.kafka;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class KafkaConsumer {

    public Map<String, String> threads = new TreeMap<>();

    private ReentrantLock lock = new ReentrantLock();

    private ConcurrentHashMap<String, KafkaThreadData> map = new ConcurrentHashMap<>();

    private void addThread(String id, String partition) {
        lock.lock();
        try {
            threads.putIfAbsent(id, partition);
        } finally {
            lock.unlock();
        }
    }

    private void addThreadToSyncColl(String id, String partition) {
        if( map.contains(id) ){
            map.get(id).addPartition(partition);
        }else {
            KafkaThreadData ktd = new KafkaThreadData();
            ktd.addPartition(partition);
            map.putIfAbsent(id, ktd);
        }

    }

    @KafkaListener(topics = "t-1-1", groupId = "jtsolv-group-id-4", concurrency = "10")
    public void listen(String message) {
        handleMessage( message,"");
    }

    @KafkaListener(topics = "t-5", groupId = "jtsolv-group-id-5")
    public void listen(@Header("kafka_receivedPartitionId") int partition,
                       @Header("kafka_receivedOffset") long offset,
                       String message) {
        handleMessage( message, String.valueOf(partition));
        dbg("Partition: " + partition);
        dbg("Offset: " + offset);
    }

    @KafkaListener(topics = "t-1", groupId = "jtsolv-group-id-6", concurrency = "10")
    public void listen(Message<String> message) {
        String payload = message.getPayload();
        Integer partition = message.getHeaders().get("kafka_receivedPartitionId", Integer.class);

        handleMessage( message.getPayload(), String.valueOf(partition));

        dbg("Message Partition: " + partition);

    }

    @KafkaListener(topics = "t-6", groupId = "jtsolv-group-id-7")
    public void listen(ConsumerRecord<String, String> record) {
        String message = record.value();
        int partition = record.partition();
        long offset = record.offset();
        handleMessage( message, String.valueOf(partition));
        dbg("Message Partition: " + partition);
        dbg("Message Offset: " + offset);
    }

    @KafkaListener(topics = "t-7", groupId = "jtsolv-group-id-8")
    public void listen(@Header("kafka_receivedPartitionId") int partition, String message) {
        handleMessage( message ,String.valueOf(partition));
        dbg("Message Partition: " + partition);
    }

    private void handleMessage(String message,String partition) {
        dbg("");
        dbg("");
        dbg("");
        dbg("Received message: " + message);
        dbg("Thread.currentThread().getName(): " + Thread.currentThread().getName());
        dbg("Thread.currentThread().getId(): " + Thread.currentThread().getId());
        dbg("Thread.currentThread().getThreadGroup(): " + Thread.currentThread().getThreadGroup());
        dbg("Thread.activeCount()(): " + Thread.activeCount());
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            dbg("Exception(): " + e.getMessage());

        }
        dbg("");
        dbg("");
        dbg("Threads ( start ):");
        addThreadToSyncColl(String.valueOf(Thread.currentThread().getId()), partition);
        printThreads();
        dbg("Threads ( end ):");
        dbg("");
        dbg("");
        dbg("");

    }
    private void dbg(String txt) {
        System.out. println(txt);
    }

    private void printThreads() {
        map.entrySet().stream()
                .forEach(this::printEntry);
    }

    private void printEntry(Map.Entry<String, KafkaThreadData> entry) {
        dbg("Thread id: " + entry.getKey());
        KafkaThreadData ktd = entry.getValue();
        map.entrySet().stream()
                .forEach(this::printEntry);
        ktd.getPartitions().entrySet().stream().forEach(this::printPartition);
    }

    private void printPartition(Map.Entry<String, String> entry) {
        String partition = entry.getValue();
        dbg("Partition: " + partition);
    }


}