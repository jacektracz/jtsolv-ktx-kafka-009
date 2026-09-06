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


    private ReentrantLock lock = new ReentrantLock();

    private ConcurrentHashMap<String, KafkaThreadData> map = new ConcurrentHashMap<>();

    private void addThreadDataToStorage(
            String threadId,
            String partition,
            String offset) {
        if( map.containsKey(threadId) ){
            dbg("Update message info ( start )");
            KafkaThreadData ktd = map.get(threadId);
            dbg("Update partition data:" + partition);
            ktd.addPartition(partition);
            dbg("Ktd thread id:" + ktd.getThreadId());
            dbg("Messages count:" + ktd.getMessagesCount());
            int mc = ktd.getMessagesCount() + 1;
            ktd.setMessagesCount( mc );
            ktd.addOffset(partition,offset);
            dbg("Messages count inc:" + ktd.getMessagesCount());
            dbg("Update message info ( end )");
        }else {
            dbg("Add message info ( start )");
            KafkaThreadData ktd = new KafkaThreadData();
            ktd.setThreadId(threadId);
            ktd.addPartition(partition);
            ktd.setMessagesCount( 1 );
            ktd.addOffset(partition, offset);
            map.putIfAbsent(threadId, ktd);
            dbg("Add message info ( end )");
        }

    }

    @KafkaListener(topics = "t-1-1", groupId = "jtsolv-group-id-4", concurrency = "10")
    public void listen(String message) {
        handleMessageThreadSafe( message,"","");
    }

    @KafkaListener(topics = "t-5", groupId = "jtsolv-group-id-5")
    public void listen(@Header("kafka_receivedPartitionId") int partition,
                       @Header("kafka_receivedOffset") long offset,
                       String message) {
        handleMessageThreadSafe( message,
                String.valueOf(partition),
                String.valueOf(offset));
    }

    @KafkaListener(topics = "t-1-bckp", groupId = "jtsolv-group-id-6", concurrency = "10")
    public void listen(Message<String> message) {
        String payload = message.getPayload();
        Integer partition = message.getHeaders().get("kafka_receivedPartitionId", Integer.class);
        handleMessageThreadSafe( message.getPayload(), String.valueOf(partition),"NO-OFFSET-INFO");
    }

    @KafkaListener(topics = "t-1", groupId = "jtsolv-group-id-7", concurrency = "10")
    public void listen(ConsumerRecord<String, String> record) {
        String message = record.value();
        int partition = record.partition();
        long offset = record.offset();
        handleMessageThreadSafe( message, String.valueOf(partition),String.valueOf(offset));
    }

    @KafkaListener(topics = "t-7", groupId = "jtsolv-group-id-8")
    public void listen(@Header("kafka_receivedPartitionId") int partition, String message) {
        handleMessageThreadSafe( message ,String.valueOf(partition),"");
        dbg("Message Partition: " + partition);
    }

    private void handleMessageThreadSafe(String message,
                                         String partition,
                                         String offset) {
        lock.lock();
        try {
            handleMessageNoThreadSafe(message, partition, offset);
        } finally {
            lock.unlock();
        }
    }

    private void handleMessageNoThreadSafe(String message,
                                           String partition,
                                           String offset) {
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

        addThreadDataToStorage(
                String.valueOf(Thread.currentThread().getId()),
                partition,
                offset);

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
        map
            .entrySet()
            .stream()
            .forEach(this::printEntry);
    }

    private void printEntry(Map.Entry<String, KafkaThreadData> entry) {

        dbg("");
        dbg("");
        dbg("-------------- THREAD (start) ------------------");
        dbg("Thread id: " + entry.getKey());
        KafkaThreadData ktd = entry.getValue();
        dbg("Messages count: " + ktd.getMessagesCount());
        dbg("Thread data id: " + ktd.getThreadId());
        dbg("Partitions count: " + ktd.getPartitions().entrySet().stream().count());

        ktd.getPartitions()
                .entrySet()
                .stream()
                .forEach(t -> printPartition(t, ktd.getThreadId()));

        dbg("-------------- THREAD (end) ------------------");
        dbg("");
        dbg("");
    }

    private void printPartition(Map.Entry<String, KafkaPartitionData> entry,String threadId) {
        KafkaPartitionData partition = entry.getValue();
        dbg("[Thread:" + threadId +  "][Partition: " + partition.getPartitionId() + "]");
        partition.getOffsets()
                .entrySet()
                .stream()
                .forEach(t -> printOffset(t,
                        threadId,
                        partition.getPartitionId()));

    }

    private void printOffset(
            Map.Entry<String, String> entry,
            String threadId,
            String partitionId) {

        dbg("[Thread:" + threadId +  "]"
                + "[Partition: " + partitionId + "]"
                + "[Offset:" + entry.getValue() + "]");

    }

}