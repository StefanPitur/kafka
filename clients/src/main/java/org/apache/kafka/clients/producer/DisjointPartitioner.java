package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DisjointPartitioner implements Partitioner {
    private final static String CFG_PRODUCERS_COUNT = "disjoint.producers.count";
    private final static String CFG_PRODUCER_INDEX = "disjoint.producer.index";

    private final ConcurrentMap<String, AtomicInteger> topicCounterMap = new ConcurrentHashMap<>();

    private int producerIndex;
    private int producersCount;

    public void configure(Map<String, ?> configs) {
        producersCount = Integer.parseInt(configs.get(CFG_PRODUCERS_COUNT).toString());
        producerIndex = Integer.parseInt(configs.get(CFG_PRODUCER_INDEX).toString());
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int nextValue = nextValue(topic);
        int numPartitions = cluster.partitionsForTopic(topic).size();

        int producerPartitionCount = getProducerPartitionCount(numPartitions);
        int producerPartitionStartIndex = getProducerPartitionStartIndex(numPartitions);

        return Utils.toPositive(nextValue) % producerPartitionCount + producerPartitionStartIndex;
    }

    private int nextValue(String topic) {
        AtomicInteger counter = topicCounterMap.computeIfAbsent(topic, k -> new AtomicInteger(0));
        return counter.getAndIncrement();
    }

    private int getProducerPartitionCount(int numPartitions) {
        int q = numPartitions / producersCount;
        int r = numPartitions % producersCount;
        return producerIndex < r ? q + 1 : q;
    }

    private int getProducerPartitionStartIndex(int numPartitions) {
        int q = numPartitions / producersCount;
        int r = numPartitions % producersCount;
        if (producerIndex < r) {
            return (q + 1) * producerIndex;
        }
        return r * (q + 1) + (producerIndex - r) * q;
    }

    public void close() {}
}
