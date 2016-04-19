package com.dangdang.tools.kafka.util;

import javafx.application.Platform;
import kafka.admin.TopicCommand;
import kafka.api.*;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.StringBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaInfoUtil {
    public static int fetchSizeBytes = 1024 * 1024;
    public static int socketTimeoutMs = 10000;
    public static int fetchMaxWait = 10000;
    public static int bufferSizeBytes = 1024 * 1024;
    public static long startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
    public static long latestOffsetTime = kafka.api.OffsetRequest.LatestTime();
    private static final int NO_OFFSET = -5;
    private Map<Integer, Broker> brokerMap = new HashMap<Integer, Broker>();
    private Map<Integer, SimpleConsumer> consumerMap = new HashMap<Integer, SimpleConsumer>();
    private KafkaProducer<String, String> kafkaProducer;
    private List<Runnable> runnableListWhenConnected = new CopyOnWriteArrayList<Runnable>();
    private String clientId = genClientId();

    private ZkClient zkClient;
    public void setKafkaZookeeperPath(String zookeeperPath) {
        if (null == zookeeperPath || zookeeperPath.isEmpty())
            return;
        if (zookeeperPath.startsWith("zookeeper://"))
            zookeeperPath = zookeeperPath.replace("zookeeper://", "");
        if (zookeeperPath.endsWith("/"))
            zookeeperPath = zookeeperPath.substring(0, zookeeperPath.length() - 1);

        String path = ZkUtils.BrokerIdsPath();
        connectZookeeper(zookeeperPath);
        if (!exists(path)) {
            AlertUtil.alert("地址不正确！提供的zookeeper地址应该类似“10.255.209.46:2181”或“10.255.209.46:2181/kafka”");
        }
        zkClient.setZkSerializer(ZKStringSerializer$.MODULE$);
        getAllBrokersInCluster();
        doRunnableListWhenConnected();
        AlertUtil.alert("连接成功");
    }

    private void connectZookeeper(String hostAndPortAndChRoot) {
        if (zkClient != null) {
            try {
                zkClient.close();
            } catch (Exception e) {
            }
            zkClient = null;
            runnableListWhenConnected.clear();
            try {
                kafkaProducer.close();
            } catch (Exception e) {
            }
            kafkaProducer = null;
            for (SimpleConsumer consumer : consumerMap.values()) {
                try {
                    consumer.close();
                } catch (Exception e) {
                }
            }
        }
        zkClient = new ZkClient(hostAndPortAndChRoot);
        brokerMap.clear();
        consumerMap.clear();
    }
    public void doWhenConnected(Runnable runnable) {
        runnableListWhenConnected.add(runnable);
    }
    private void doRunnableListWhenConnected() {
        try {
            Platform.runLater(new Runnable() {
                @Override
                public void run() {
                    for (Runnable runnable : runnableListWhenConnected) {
                        try {
                            runnable.run();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private boolean exists(String zooPath) {
        return zkClient.exists(zooPath);
    }
    private void ensureConnect() {
        if (null == zkClient) {
            AlertUtil.alert("请先连接zookeeper（点击连接按钮）");
            throw new RuntimeException("zkClient未初始化");
        }
    }
    public Map<Integer, Broker> getAllBrokersInCluster() {
        ensureConnect();
        if (brokerMap.size() > 0)
            return brokerMap;

        List<Broker> brokers = JavaConversions.asJavaList(ZkUtils.getAllBrokersInCluster(zkClient));
        for (Broker broker : brokers) {
            brokerMap.put(broker.id(), broker);
            SimpleConsumer simpleConsumer = new SimpleConsumer(broker.host(), broker.port(),
                    socketTimeoutMs, bufferSizeBytes, clientId);
            consumerMap.put(broker.id(), simpleConsumer);
        }
        return brokerMap;
    }
    public List<String> getAllTopics() {
        ensureConnect();
        return JavaConversions.asJavaList(ZkUtils.getAllTopics(zkClient));
    }
    public static List<TopicMetadata> getTopicMetadata(SimpleConsumer consumer, List<String> topics) {
        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        TopicMetadataResponse topicMetadataResponse = consumer.send(topicMetadataRequest);
        return topicMetadataResponse.topicsMetadata();
    }
    public List<TopicMetadata> getTopicMetadata(List<String> topics) {
        ensureConnect();
        List<TopicMetadata> resultList = new LinkedList<TopicMetadata>();
        if (consumerMap.isEmpty())
            return resultList;

        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(topics);
        TopicMetadataResponse topicMetadataResponse = consumerMap.values().iterator().next().send(topicMetadataRequest);
        resultList.addAll(topicMetadataResponse.topicsMetadata());
        return resultList;
    }
    public TopicMetadata getTopicMetadata(String topic) {
        List<TopicMetadata> list = getTopicMetadata(Arrays.asList(topic));
        if (null == list || list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }
    public Set<TopicAndPartition> getAllPartitions() {
        ensureConnect();
        return JavaConversions.asJavaSet(ZkUtils.getAllPartitions(zkClient));
    }
    public String describeTopic() {
        ensureConnect();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
        TopicCommand.describeTopic(zkClient, new TopicCommand.TopicCommandOptions(new String[] { "describe" }));
        System.setOut(originalOut);
        return new String(outputStream.toByteArray());
    }
    public String listTopic() {
        ensureConnect();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
        TopicCommand.listTopics(zkClient, new TopicCommand.TopicCommandOptions(new String[] { "list" }));
        System.setOut(originalOut);
        return new String(outputStream.toByteArray());
    }
    private static String genClientId() {
        return "KafkaInfoUtil_" + System.currentTimeMillis();
    }

    public long[] getOffset(kafka.javaapi.PartitionMetadata partitionMetadata, String topic) {
        SimpleConsumer consumer = consumerMap.get(partitionMetadata.leader().id());
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionMetadata.partitionId());
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(latestOffsetTime, 2));
        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partitionMetadata.partitionId());
        if (offsets.length > 0) {
            return offsets; // 如果该分区有消息，则offsets的长度为2，元素分别是最大、最小offset
        } else {
            return null;
        }
    }

    public static long getOffset(SimpleConsumer consumer, String topic, int partition, long startOffsetTime) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(startOffsetTime, 2));
        OffsetRequest request = new OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

        long[] offsets = consumer.getOffsetsBefore(request).offsets(topic, partition);
        if (offsets.length > 0) {
            System.out.printf("---" + offsets[0]);
            if (offsets.length > 1)
                System.out.printf("," + offsets[1]);
            System.out.print(":");
            return offsets[0];
        } else {
            return NO_OFFSET;
        }
    }

    public ByteBufferMessageSet fetchMessages(PartitionMetadata partitionMetadata, String topic, long startOffset) {
//        Broker broker = brokerMap.get(partitionMetadata.leader().id());
//        SimpleConsumer simpleConsumer = new SimpleConsumer(broker.host(), broker.port(),
//                socketTimeoutMs, bufferSizeBytes, clientId);
        SimpleConsumer consumer = consumerMap.get(partitionMetadata.leader().id());
        ByteBufferMessageSet messageSet = fetchMessages(consumer, topic, partitionMetadata.partitionId(), startOffset);
//        simpleConsumer.close();
        return messageSet;
    }

    public static ByteBufferMessageSet fetchMessages(SimpleConsumer consumer, String topic, int partitionId, long offset) {
        ByteBufferMessageSet msgs = null;
        kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partitionId, offset, fetchSizeBytes).
                clientId(consumer.clientId()).maxWait(fetchMaxWait).build();
        FetchResponse fetchResponse;
        try {
            fetchResponse = consumer.fetch(fetchRequest);
        } catch (Exception e) {
            if (e instanceof ConnectException ||
                    e instanceof SocketTimeoutException ||
                    e instanceof IOException ||
                    e instanceof UnresolvedAddressException
                    ) {
                throw new RuntimeException("Network error when fetching messages", e);
            } else {
                throw new RuntimeException(e);
            }
        }
        if (fetchResponse.hasError()) {
            KafkaError error = KafkaError.getError(fetchResponse.errorCode(topic, partitionId));
            if (error.equals(KafkaError.OFFSET_OUT_OF_RANGE)) {
                throw new RuntimeException("Got fetch request with offset out of range: [" + offset + "]");
            } else {
                String message = "Error fetching data from [" + partitionId + "] for topic [" + topic + "]: [" + error + "]";
                throw new RuntimeException(message);
            }
        } else {
            msgs = fetchResponse.messageSet(topic, partitionId);
        }
        return msgs;
    }
    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }
    public static String toString(ByteBuffer buffer) {
        return new String(toByteArray(buffer));
    }
    public static String toString(MessageAndOffset messageAndOffset) {
        StringBuilder sb = new StringBuilder();
        if (messageAndOffset == null) {
            return sb.toString();
        }
        sb.append("message start");
        if (messageAndOffset.message().hasKey()) {
            sb.append("key: ").append(toString(messageAndOffset.message().key())).append("\n");
        }
        sb.append("offset: ").append(messageAndOffset.offset()).append("\n");
        sb.append("message: ").append(toString(messageAndOffset.message().payload())).append("\n");
        sb.append("message end");

        return sb.toString();
    }

    private void ensureProducer() {
        if (null != kafkaProducer) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (Broker broker : getAllBrokersInCluster().values()) {
            if (sb.length() > 0)
                sb.append(",");
            sb.append(broker.host()).append(":").append(broker.port());
        }
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, sb.toString()); // 10.255.209.46:9092
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try {
            kafkaProducer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            kafkaProducer = null;
            throw new RuntimeException(e);
        }
    }

    public void sendMessage(String topic, Integer partition, String key, final String value) {
        ensureProducer();
        kafkaProducer.send(new ProducerRecord(topic, partition, key, value));
        AlertUtil.alert("发送消息成功：" + value);
    }

    public List<String> getConsumerGroups() {
        return zkClient.getChildren(ZkUtils.ConsumersPath());
    }

    /**返回offset，long转为字符串；如果没有查到，则返回null*/
    public String getConsumerOffset(String group, String topic, Integer partitionId) {
        String path = String.format("%s/%s/offsets/%s/%s", ZkUtils.ConsumersPath(), group, topic, partitionId);
        Tuple2<Option<String>, org.apache.zookeeper.data.Stat> tuple2 = ZkUtils.readDataMaybeNull(zkClient, path);
        if (tuple2._1().isEmpty())
            return null;
        return tuple2._1().get();
    }

    public static void main( String[] args ) throws Exception {
        KafkaInfoUtil kafkaInfoUtil = new KafkaInfoUtil();
        kafkaInfoUtil.setKafkaZookeeperPath("10.255.209.46:2181/kafka/");
        SimpleConsumer simpleConsumer = kafkaInfoUtil.consumerMap.get(1);
//        SimpleConsumer simpleConsumer2 = kafkaInfoUtil.consumerMap.get(2);
        System.out.println(kafkaInfoUtil.zkClient.getChildren(ZkUtils.ConsumersPath()));
        System.out.println(kafkaInfoUtil.zkClient.getChildren(ZkUtils.ConsumersPath() + "/louzy-study-valid/offsets/louzy-study-kafka"));
        System.out.println(kafkaInfoUtil.zkClient.getChildren(ZkUtils.ConsumersPath() + "/louzy-study-valid/offsets/louzy-study-kafka"));
        for (int i = 0; i < 5; i++) {
            String path = ZkUtils.ConsumersPath() + "/louzy-study-valid/offsets/louzy-study-kafka/" + i;
            Tuple2<Option<String>, org.apache.zookeeper.data.Stat> tuple2 = ZkUtils.readDataMaybeNull(kafkaInfoUtil.zkClient, path);
            System.out.println(tuple2._1().isEmpty() + ", " + tuple2._2());
        }

//        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.255.209.46:9092");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//        producer.send(new ProducerRecord("test1", null, null, "...4"));
//        producer.send(new ProducerRecord("test1", null, null, "...5"));
//        producer.close();



//        System.out.println(getOffset(simpleConsumer, "orderEventTopic", 0, startOffsetTime) + "," + getOffset(simpleConsumer2, "orderEventTopic", 0, startOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderEventTopic", 0, latestOffsetTime) + "," + getOffset(simpleConsumer2, "orderEventTopic", 0, latestOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 0, startOffsetTime) + "," + getOffset(simpleConsumer2, "orderNonEventTopic", 0, startOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 0, latestOffsetTime) + "," + getOffset(simpleConsumer2, "orderNonEventTopic", 0, latestOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 5, startOffsetTime) + "," + getOffset(simpleConsumer2, "orderNonEventTopic", 5, startOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 5, latestOffsetTime) + "," + getOffset(simpleConsumer2, "orderNonEventTopic", 5, latestOffsetTime));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 1, startOffsetTime) + "," + getOffset(simpleConsumer2, "orderNonEventTopic", 1, latestOffsetTime));
//        System.out.println("======");
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 5, System.currentTimeMillis()));
//        System.out.println(getOffset(simpleConsumer, "orderNonEventTopic", 5, -1));
//
//        int offset = 21551;
//        int cnt = 0;
//        while (true) {
//            ByteBufferMessageSet messageSet = fetchMessages(simpleConsumer, "orderNonEventTopic", 5, offset);
//            if (null == messageSet || !messageSet.iterator().hasNext())
//                break;
//
//            for (MessageAndOffset messageAndOffset : messageSet) {
//                if (messageAndOffset.offset() != offset) {
//                    System.out.println("error!!");
//                    break;
//                }
//                offset++;
//                cnt++;
////            System.out.println(toString(messageAndOffset));
//            }
//        }
//        System.out.println("cnt:" + cnt);
//        System.out.println("offset:" + offset);


//        for (int i = 0; i < 8; i++) {
//            System.out.println("orderNonEventTopic_" + i + ":" + getOffset(simpleConsumer, "orderNonEventTopic", i, startOffsetTime)
//                    + "," + getOffset(simpleConsumer2, "orderNonEventTopic", i, startOffsetTime));
//            System.out.println("orderNonEventTopic_" + i + ":" + getOffset(simpleConsumer, "orderNonEventTopic", i, latestOffsetTime)
//                + "," + getOffset(simpleConsumer2, "orderNonEventTopic", i, latestOffsetTime));
//        }

//        System.out.println(getTopicMetadata(simpleConsumer, Arrays.asList("orderNonEventTopic", "orderEventTopic")));
//        System.out.println(kafkaInfoUtil.getAllBrokersInCluster());
//        System.out.println(kafkaInfoUtil.getAllPartitions());
//        System.out.println("============");
//        System.out.println(kafkaInfoUtil.describeTopic());
//        System.out.println("============");
//        System.out.println(kafkaInfoUtil.listTopic());
    }
}
