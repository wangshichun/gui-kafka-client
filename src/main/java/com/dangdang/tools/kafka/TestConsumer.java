package com.dangdang.tools.kafka;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.utils.IteratorTemplate;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by wangshichun on 2016/5/13.
 */
public class TestConsumer {
    private final ConsumerConnector consumer;
    private final String topic;
    private  ExecutorService executor;

    public TestConsumer(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) consumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("auto.commit.enable", "false");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
        String zooKeeper = "10.255.209.46:2181,10.255.209.47:2181/kafka";
        String groupId = "test";
        String topic = "test"; // topic的名称
        int threads = 1; // kafka的分区数

        TestConsumer example = new TestConsumer(zooKeeper, groupId, topic);
        example.run(threads);

        try {
            new CountDownLatch(1).await();
        } catch (InterruptedException ie) {
        }

        example.shutdown();
    }

    public class ConsumerTest implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;

        public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
            while (it.hasNext()) {
//                try {
//                    Field consumedOffset = ConsumerIterator.class.getDeclaredField("consumedOffset");
//                    consumedOffset.setAccessible(true);
//                    Field nextItem = IteratorTemplate.class.getDeclaredField("nextItem");
//                    nextItem.setAccessible(true);
//
//                    MessageAndMetadata<byte[], byte[]> messageAndMetadata = (MessageAndMetadata<byte[], byte[]>)nextItem.get(it);
//                    consumedOffset.set(it, messageAndMetadata.offset());
//
//                    System.out.println(new String(messageAndMetadata.message()));
//                    System.out.println(new String(messageAndMetadata.message()));
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
                MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
                System.out.println("Thread " + m_threadNumber + ": " + new String(messageAndMetadata.message()));
            }
            System.out.println("Shutting down Thread: " + m_threadNumber);
        }
    }
}