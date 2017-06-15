package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.AlertUtil;
import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Orientation;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Region;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageTail extends Tab {
    protected KafkaInfoUtil kafkaInfoUtil;
    protected ComboBox<String> topicComboBox;
    protected ComboBox<String> topicPartitionComboBox;
    private TextField tailNumberTextField;
    private TextArea tailResultText;
    private Thread tailThread = null;
    private AtomicBoolean shouldRunFlag = new AtomicBoolean(true);
    private static final String ALL_PARTITION = "全部";

    public KafkaMessageTail(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        this.setClosable(false);
        this.setText("  tail -f  ");
        this.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + ";");
        this.kafkaInfoUtil = kafkaInfoUtil;
        FlowPane pane = new FlowPane(Orientation.HORIZONTAL, 5, 5);
        JavaFxUtil.setPreWidth(tabPane, new Region[] { pane });
        JavaFxUtil.addWidthListener(tabPane, new Region[] { pane });
        pane.setStyle("-fx-padding: 5px;");
        this.setContent(pane);

        addTopicComboBox(pane);
        JavaFxUtil.addNewSpace(pane, 2);
        addTopicPartitionComboBox(pane);
        JavaFxUtil.addNewSpace(pane, 4);
        addGetAllTopicsButton(pane);

        JavaFxUtil.addNewLine(pane);
        addFilterControl(pane);
        addTailControl(pane);

        kafkaInfoUtil.doWhenConnected(new Runnable() {
            @Override
            public void run() {
                getTopicsAndDisplay();
            }
        });
    }

    protected void addFilterControl(FlowPane pane) {
    }

    protected void addTailControl(FlowPane pane) {
        JavaFxUtil.addNewLine(pane);
        addTailTextfield(pane);
        JavaFxUtil.addNewSpace(pane, 4);
        addEnterButton(pane);
        addBreakButton(pane);
        JavaFxUtil.addNewLine(pane);
        addTailResultControl(pane);
    }

    private void addTailResultControl(final FlowPane pane) {
        tailResultText = new TextArea();
        tailResultText.setPrefWidth(JavaFxUtil.screenSize.getWidth() / 2);
        tailResultText.setPrefHeight(430);
        tailResultText.setEditable(false);
        tailResultText.setWrapText(true);
//        ScrollPane scrollPane = new ScrollPane(tailResultText);
//        scrollPane.setPrefViewportHeight(500);
//        scrollPane.setPrefViewportWidth(tailResultText.getPrefWidth() + 20);
        pane.getChildren().add(tailResultText);
    }

    private void addTailTextfield(FlowPane pane) {
        tailNumberTextField = new TextField("100");
        tailNumberTextField.setPrefColumnCount(5);
        tailNumberTextField.setAlignment(Pos.CENTER_RIGHT);
        tailNumberTextField.setPadding(new Insets(0));
        pane.getChildren().addAll(new Label("tail -"), tailNumberTextField, new Label("f"));
    }

    private void addTopicComboBox(FlowPane pane) {
        topicComboBox = new ComboBox<String>();
        pane.getChildren().addAll(new Label("topic："), topicComboBox);
        topicComboBox.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                getTopicPartitionsAndDisplay(topicComboBox.getSelectionModel().getSelectedItem());
            }
        });
    }
    private void addTopicPartitionComboBox(FlowPane pane) {
        topicPartitionComboBox = new ComboBox<String>();
        pane.getChildren().addAll(new Label("分区："), topicPartitionComboBox);
    }

    private void getTopicPartitionsAndDisplay(String topic) {
        if (null == topic || topic.isEmpty())
            return;

        String par = topicPartitionComboBox.getSelectionModel().getSelectedItem();
        topicPartitionComboBox.getItems().clear();
        TopicMetadata meta = kafkaInfoUtil.getTopicMetadata(topic);
        if (null == meta || meta.partitionsMetadata().isEmpty())
            return;
        List<String> partitionList = new LinkedList<String>();
        partitionList.add(ALL_PARTITION);
        for (PartitionMetadata partitionMetadata : meta.partitionsMetadata()) {
            partitionList.add(String.valueOf(partitionMetadata.partitionId()));
        }
        topicPartitionComboBox.getItems().addAll(partitionList);
        if (null != par)
            topicPartitionComboBox.getSelectionModel().select(par);
        if (topicPartitionComboBox.getSelectionModel().getSelectedIndex() < 0)
            topicPartitionComboBox.getSelectionModel().select(0);
    }

    private void addBreakButton(FlowPane pane) {
        Button button = new Button("停止(Ctrl + C)");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                stop();
            }
        });
    }

    private void startTail() {
        stop(); // 原来如果有tail线程，先停止

        // 检查topic是否已选择
        final String topic = topicComboBox.getSelectionModel().getSelectedItem();
        if (topic == null || topic.isEmpty()) {
            AlertUtil.alert("请先选中topic");
            return;
        }

        // 计算所选分区
        final List<Integer> partitionList = new LinkedList<Integer>();
        if (ALL_PARTITION.equals(topicPartitionComboBox.getSelectionModel().getSelectedItem())) {
            for (int i = 1; i < topicPartitionComboBox.getItems().size(); i++) {
                partitionList.add(Integer.valueOf(topicPartitionComboBox.getItems().get(i)));
            }
        } else {
            partitionList.add(Integer.valueOf(topicPartitionComboBox.getSelectionModel().getSelectedItem()));
        }
        if (partitionList.isEmpty())
            return;

        Integer count = 0;
        try {
            count = Integer.valueOf(tailNumberTextField.getText());
        } catch (Exception e) {}
        if (count < 1) {
            AlertUtil.alert("请输入tail的消息数");
            return;
        }

        // 开始获取消息
        final Integer finalCount = count;
        tailThread = new Thread(new Runnable() {
            @Override
            public void run() {
                tail(topic, partitionList, finalCount);
            }
        });
        tailThread.start();
    }

    private void tail(String topic, List<Integer> partitionList, Integer tailCount) {
        tailResultText.clear();
        shouldRunFlag.set(true);
        TopicMetadata topicMeta = kafkaInfoUtil.getTopicMetadata(topic);
        if (null == topicMeta) {
            return;
        }


        // 计算总消息数
        int cnt = 0;
        final Map<Integer, Long> startOffsetMap = new HashMap<Integer, Long>();
        final Map<Integer, Long> endOffsetMap = new HashMap<Integer, Long>();
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            if (!partitionList.contains(meta.partitionId()))
                continue;

            long count = 0;
            long[] offset = kafkaInfoUtil.getOffset(meta, topic);
            if (offset != null && offset.length == 2)
                count = offset[0] - offset[1];
            if (count < 1)
                continue;

            cnt += count;
            startOffsetMap.put(meta.partitionId(), offset[1]);
            endOffsetMap.put(meta.partitionId(), offset[0]);
        }

        // 判断是否需要从中间的offset开始读
        if (null != tailCount && tailCount < cnt) {
            double percentage = tailCount * 1.0 / cnt;
            for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
                if (!startOffsetMap.containsKey(meta.partitionId()))
                    continue;
                long end = endOffsetMap.get(meta.partitionId());
                long start = startOffsetMap.get(meta.partitionId());
                long count = end - start;
                start = end - Double.valueOf(count * percentage + 1).longValue();
                startOffsetMap.put(meta.partitionId(), start);
            }
        }

        List<String> keyList = new LinkedList<String>();
        List<String> messageList = new LinkedList<String>();
        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<Integer, PartitionMetadata>();
        for (PartitionMetadata metadata : topicMeta.partitionsMetadata()) {
            partitionMetadataMap.put(metadata.partitionId(), metadata);
        }
        Collections.sort(partitionList);

        int totalMessageCount = 0;
        while (true) {
            if (shouldRunFlag.get() == false)
                break;
            cnt = 0;
            try {
                for (Integer partitionId : partitionList) {
                    PartitionMetadata meta = partitionMetadataMap.get(partitionId);
                    if (meta == null || !partitionList.contains(partitionId) || !startOffsetMap.containsKey(partitionId))
                        continue;

                    ByteBufferMessageSet messageSet = kafkaInfoUtil.fetchMessages(meta, topic, startOffsetMap.get(partitionId));
                    if (messageSet == null || !messageSet.iterator().hasNext())
                        continue;

                    int setSize = 0;
                    for (MessageAndOffset messageAndOffset : messageSet) {
                        String key = null;
                        String msg = KafkaInfoUtil.toString(messageAndOffset.message().payload());
                        if (messageAndOffset.message().hasKey()) {
                            key = KafkaInfoUtil.toString(messageAndOffset.message().key());
                        }
                        if (shouldSkip(key, msg))
                            continue;

                        setSize++;
                        totalMessageCount++;
                        if (shouldStop(key, msg, totalMessageCount))
                            continue;
                        if (messageAndOffset.message().hasKey()) {
                            keyList.add(key);
                            if (keyList.size() > tailCount)
                                keyList.remove(0);
                        }
                        messageList.add(msg);
                        if (messageList.size() > tailCount)
                            messageList.remove(0);

                        if (messageAndOffset.nextOffset() > startOffsetMap.get(partitionId)) {
                            startOffsetMap.put(partitionId, messageAndOffset.nextOffset());
                        }
                    }
                    cnt += setSize;
                    if (setSize > 0 && messageList.size() > 0) {
                        receiveNewMessage(keyList, messageList, tailResultText);
                    }
                }
                if (cnt < 1) {
                    Thread.sleep(50);
                }
            } catch (InterruptedException e) {
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected boolean shouldSkip(String key, String msg) {
        return false;
    }

    protected boolean shouldStop(String key, String msg, int totalMessageCount) {
        return false;
    }

    private void receiveNewMessage(List<String> keyList, List<String> messageList, TextArea tailResultText) {
        if (messageList.isEmpty())
            return;

        StringBuilder sb = new StringBuilder();
        if (keyList.size() == messageList.size()) {
            for (int i = 0; i < messageList.size(); i++) {
                if (sb.length() > 0)
                    sb.append("\n");
                sb.append("key: ").append(keyList.get(i)).append(", ");
                sb.append("value: ").append(messageList.get(i));
            }
        } else {
            for (int i = 0; i < messageList.size(); i++) {
                if (sb.length() > 0)
                    sb.append("\n");
                sb.append(messageList.get(i));
            }
        }

        tailResultText.setText(sb.toString());
        tailResultText.positionCaret(tailResultText.getLength());
    }

    private void stop() {
        if (tailThread != null) {
            shouldRunFlag.set(false);
            tailThread = null;
        }
    }

    private void addEnterButton(FlowPane pane) {
        Button button = new Button("开始(enter)");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                startTail();
            }
        });
    }
    private void addGetAllTopicsButton(FlowPane pane) {
        Button button = new Button("刷新topic列表");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;
                getTopicsAndDisplay();
            }
        });
    }
    private void getTopicsAndDisplay() {
        String topic = topicComboBox.getSelectionModel().getSelectedItem();
        topicComboBox.getItems().clear();
        List<String> list = kafkaInfoUtil.getAllTopics();
        Collections.sort(list);
        list.remove("__consumer_offsets");
        topicComboBox.getItems().addAll(list);
        if (topic != null && topic.length() > 0) {
            topicComboBox.getSelectionModel().select(topic);
        }
        if (topicComboBox.getSelectionModel().getSelectedIndex() < 0) {
            topicComboBox.getSelectionModel().select(0);
        }
    }
}
