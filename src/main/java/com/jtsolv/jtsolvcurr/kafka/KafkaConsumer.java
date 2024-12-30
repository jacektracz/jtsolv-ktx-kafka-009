package com.jtsolv.jtsolvcurr.kafka;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

@Component
public class KafkaConsumer {

    public Map<String,String> threads = new TreeMap<>();

    private ReentrantLock lock  = new ReentrantLock();

    private ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

    private void addThread(String id,String partition){

        lock.lock();
        try {

        }finally {
            lock.unlock();
        }
    }

    private void addThreadEx(String id,String partition){
        map.putIfAbsent(id,partition);
    }

    @KafkaListener(topics = "t-1", groupId = "jtsolv-group-id-4",concurrency = "10")
    public void listen(String message) {
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
        printThreads();
        dbg("Threads ( end ):");
        dbg("");
        dbg("");
        dbg("");

    }

    private void dbg(String txt){
        System.out.println(txt);
    }

    private void printThreads(){
        map.entrySet().stream()
                .forEach(this::printEntry );
    }

    private void printEntry(Map.Entry<String,String> entry){
        dbg("Thread id: " + entry.getKey() + ", Partition: " + entry.getValue());
    }

}