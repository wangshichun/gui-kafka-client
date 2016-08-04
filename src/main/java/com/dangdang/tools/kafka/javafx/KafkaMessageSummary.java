package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.AlertUtil;
import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.geometry.Orientation;
import javafx.scene.chart.*;
import javafx.scene.control.*;
import javafx.scene.control.cell.MapValueFactory;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Region;
import javafx.stage.FileChooser;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageSummary extends Tab {
    private KafkaInfoUtil kafkaInfoUtil;
    private ComboBox<String> topicComboBox;
    private BarChart<String, Integer> partitionsChart;
    private TableView<Map> resultTable;
    private TextField stormZookeeper;
    public KafkaMessageSummary(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        this.setClosable(false);
        this.setText("  分区消息统计、下载  ");
        this.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + ";");
        this.kafkaInfoUtil = kafkaInfoUtil;
        FlowPane pane = new FlowPane(Orientation.HORIZONTAL, 5, 5);
        JavaFxUtil.setPreWidth(tabPane, new Region[] { pane });
        JavaFxUtil.addWidthListener(tabPane, new Region[] { pane });
        pane.setStyle("-fx-padding: 5px;");
        this.setContent(pane);

        addTopicComboBox(pane);
        JavaFxUtil.addNewSpace(pane, 2);
        addGetSummaryButton(pane);
        JavaFxUtil.addNewLine(pane);
        addSummaryResultChart(pane);

        JavaFxUtil.addNewLine(pane);
        addDownloadAllButton(pane);
        JavaFxUtil.addNewSpace(pane, 2);
        addDownloadLatestButton(pane);

        kafkaInfoUtil.doWhenConnected(new Runnable() {
            @Override
            public void run() {
                getTopicsAndDisplay();
                getSummaryAndDisplay(topicComboBox.getSelectionModel().getSelectedItem());
            }
        });
    }
    private void addDownloadLatestButton(FlowPane pane) {
        Button button = new Button("下载此topic最新的 n 条消息");
        pane.getChildren().add(button);
        final TextField textField = new TextField("100");
        textField.setPrefColumnCount(10);
        pane.getChildren().add(textField);

//        stormZookeeper = new TextField("10.255.209.46:2181,10.255.209.47:2181");
//        stormZookeeper.setPrefColumnCount(20);
//        pane.getChildren().addAll(new Label("storm的zookeeper"), stormZookeeper);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                String topic = topicComboBox.getSelectionModel().getSelectedItem();
                if (topic == null || topic.isEmpty()) {
                    AlertUtil.alert("请先选中topic");
                    return;
                }
                int count = 0;
                try {
                    count = Integer.valueOf(textField.getText());
                } catch (Exception e) {
                }
                if (count < 1) {
                    AlertUtil.alert("请输入要下载的最新消息数");
                    return;
                }

                FileChooser fileChooser = new FileChooser();
                fileChooser.setTitle("选择消息保存的文件名");
                fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("文本文件", "txt"));
                File file = fileChooser.showSaveDialog(topicComboBox.getScene().getWindow());
                if (file == null)
                    return;
                downloadMessage(topic, file, count);
            }
        });
    }
    private void addDownloadAllButton(FlowPane pane) {
        Button button = new Button("下载此topic全部消息");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                String topic = topicComboBox.getSelectionModel().getSelectedItem();
                if (topic == null || topic.isEmpty()) {
                    AlertUtil.alert("请先选中topic");
                    return;
                }
                FileChooser fileChooser = new FileChooser();
                fileChooser.setTitle("选择消息保存的文件名");
                fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("文本文件", "txt"));
                File file = fileChooser.showSaveDialog(topicComboBox.getScene().getWindow());
                if (file == null)
                    return;
                downloadMessage(topic, file, null);
            }
        });
    }
    private void downloadMessage(final String topic, final File outputFile, Integer totalCount) {
        TopicMetadata topicMeta = kafkaInfoUtil.getTopicMetadata(topic);
        if (null == topicMeta) {
            return;
        }

        // 计算总消息数
        int cnt = 0;
        final Map<Integer, Long> startOffsetMap = new HashMap<Integer, Long>();
        final Map<Integer, Long> endOffsetMap = new HashMap<Integer, Long>();
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
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
        if (null != totalCount && totalCount < cnt) {
            double percentage = totalCount * 1.0 / cnt;
            for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
                if (!startOffsetMap.containsKey(meta.partitionId()))
                    continue;
                long end = endOffsetMap.get(meta.partitionId());
                long start = startOffsetMap.get(meta.partitionId());
                long count = end - start;
                start = end - Double.valueOf(count * percentage).longValue();
                startOffsetMap.put(meta.partitionId(), start);
            }
        }

        // 下载文件
        for (final PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            if (!startOffsetMap.containsKey(meta.partitionId()))
                continue;

            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        String path = outputFile.getAbsolutePath().contains(".txt") ?
                                outputFile.getAbsolutePath().replace(".txt", "-" + meta.partitionId() + ".txt") :
                                outputFile.getAbsolutePath() + "-" + meta.partitionId() + ".txt";
                        fetchMessagesToFile(meta, topic, startOffsetMap.get(meta.partitionId()), endOffsetMap.get(meta.partitionId()),
                                new File(path));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }
    private void fetchMessagesToFile(PartitionMetadata partitionMetadata, String topic, long startOffset, long endOffset, File file) {
        ByteBufferMessageSet messageSet;
        BufferedWriter writer = null;
        boolean success = true;
        try {
            new File(file.getAbsolutePath() + ".complete").delete();
        } catch (Exception e3) {
        }
        try {
            writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
            while (startOffset < endOffset) {
                messageSet = kafkaInfoUtil.fetchMessages(partitionMetadata, topic, startOffset);
                if (messageSet == null || !messageSet.iterator().hasNext())
                    break;

                for (MessageAndOffset messageAndOffset : messageSet) {
                    String msg = KafkaInfoUtil.toString(messageAndOffset.message().payload());
                    writer.write(msg);
                    writer.write("\n");
                    if (messageAndOffset.offset() >= startOffset)
                        startOffset = messageAndOffset.nextOffset();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            success = false;
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception e2) {
                }
            }
            try {
                new File(file.getAbsolutePath() + ".complete").createNewFile();
            } catch (Exception e3) {
            }
            if (success) {
                AlertUtil.alert(String.format("topic %s的分区 %s 的消息下载完毕", topic, partitionMetadata.partitionId()));
            }
        }
    }

    private void addSummaryResultChart(FlowPane pane) {
        CategoryAxis xAxis = new CategoryAxis();
        NumberAxis yAxis = new NumberAxis();
        XYChart.Series series = new XYChart.Series();
        partitionsChart = new BarChart(xAxis, yAxis);
        partitionsChart.getXAxis().setLabel("topic的分区 (消息数)");
        partitionsChart.getYAxis().setLabel("消息数量");
        partitionsChart.setTitle("各分区的消息数统计");
        partitionsChart.getData().addAll(series);
        partitionsChart.setAnimated(true);
        partitionsChart.setPrefHeight(250);
        JavaFxUtil.setPreWidth(pane, new Region[] {partitionsChart});

        resultTable = new TableView<Map>();
        resultTable.setStyle("-fx-padding: 0px;");
        resultTable.setPrefHeight(250);
        JavaFxUtil.setPreWidth(pane, new Region[] { resultTable });

        SplitPane splitPane = new SplitPane(partitionsChart, resultTable);
        splitPane.setOrientation(Orientation.VERTICAL);
        splitPane.setPrefHeight(500);
        JavaFxUtil.setPreWidth(pane, new Region[] { splitPane });
        pane.getChildren().addAll(splitPane);
//        pane.getChildren().addAll(partitionsChart, resultTable);
    }

    private void addTopicComboBox(FlowPane pane) {
        topicComboBox = new ComboBox<String>();
        pane.getChildren().add(topicComboBox);
        topicComboBox.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                getSummaryAndDisplay(topicComboBox.getSelectionModel().getSelectedItem());
            }
        });
    }

    private void addGetSummaryButton(FlowPane pane) {
        Button button = new Button("刷新");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                getTopicsAndDisplay();
                getSummaryAndDisplay(topicComboBox.getSelectionModel().getSelectedItem());
            }
        });
    }
    private void getTopicsAndDisplay() {
        String topic = topicComboBox.getSelectionModel().getSelectedItem();
        topicComboBox.getItems().clear();
        List<String> list = kafkaInfoUtil.getAllTopics();
        list.remove("__consumer_offsets");
        Collections.sort(list);
        topicComboBox.getItems().addAll(list);
        if (topic != null && topic.length() > 0) {
            topicComboBox.getSelectionModel().select(topic);
        }
        if (topicComboBox.getSelectionModel().getSelectedIndex() < 0) {
            topicComboBox.getSelectionModel().select(0);
        }
    }
    private void getSummaryAndDisplay(String topic) {
        TopicMetadata topicMeta = kafkaInfoUtil.getTopicMetadata(topic);
        if (null == topicMeta) {
            return;
        }

        XYChart.Series<String, Integer> series = partitionsChart.getData().get(0);
        List<XYChart.Data> list = new LinkedList<XYChart.Data>();
        long total = 0;
        Map<Integer, Long> startOffsetMap = new HashMap<Integer, Long>();
        Map<Integer, Long> endOffsetMap = new HashMap<Integer, Long>();
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            long count = 0;
            long[] offset = kafkaInfoUtil.getOffset(meta, topic);
            if (offset != null && offset.length > 0) {
                endOffsetMap.put(meta.partitionId(), offset[0]);
                if (offset.length > 1) {
                    startOffsetMap.put(meta.partitionId(), offset[1]);
                    count = offset[0] - offset[1];
                } else {
                    startOffsetMap.put(meta.partitionId(), offset[0]);
                }
            }
            total += count;
            String partition = "分区" + meta.partitionId() + " (" + count + ")";
            XYChart.Data data = new XYChart.Data(partition, count);
            list.add(data);
        }
        series.setName(topic + " (" + total + ")");
        partitionsChart.setVisible(false);
        series.getData().setAll(list.toArray(new XYChart.Data[list.size()]));
        partitionsChart.setVisible(true);

        getConsumerSummaryAndDisplay(topicMeta, startOffsetMap, endOffsetMap);
    }

    private void getConsumerSummaryAndDisplay(TopicMetadata topicMeta, Map<Integer, Long> startOffsetMap, Map<Integer, Long> endOffsetMap) {
        resultTable.setVisible(false);
        // 设置列名称
        final String NAME_COLUMN = "name";
        List<TableColumn<Map, String>> columnList = new ArrayList<TableColumn<Map, String>>();
        TableColumn<Map, String> column = new TableColumn<Map, String>("消费者group名称");
        column.setCellValueFactory(new MapValueFactory<String>(NAME_COLUMN));
        columnList.add(column);
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            column = new TableColumn<Map, String>("分区" + meta.partitionId());
            column.setCellValueFactory(new MapValueFactory<String>(meta.partitionId()));
            columnList.add(column);
        }
        resultTable.getColumns().setAll(columnList);

        // 填充数据
        List<Map<Object, String>> dataList = new LinkedList<Map<Object, String>>();
        // 起始offset
        Map<Object, String>
                row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "leader broker");
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            if (meta.leader() == null)
                continue;
            row.put(meta.partitionId(), String.valueOf(meta.leader().id()));
        }
        dataList.add(row);

        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "replica brokers");
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            List<Broker> replicas = meta.replicas();
            StringBuilder sb = new StringBuilder();
            for (Broker broker : replicas) {
                if (sb.length() > 0) sb.append(", ");
                sb.append(broker.id());
            }
            row.put(meta.partitionId(), sb.toString());
        }
        dataList.add(row);

        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "startOffset");
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            row.put(meta.partitionId(), startOffsetMap.getOrDefault(meta.partitionId(), -1L).toString());
        }
        dataList.add(row);

        // 结束offset
        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "endOffset");
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            row.put(meta.partitionId(), endOffsetMap.getOrDefault(meta.partitionId(), -1L).toString());
        }
        dataList.add(row);

        // size
        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "消息数");
        for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
            row.put(meta.partitionId(), String.valueOf(endOffsetMap.getOrDefault(meta.partitionId(), 0L) - startOffsetMap.getOrDefault(meta.partitionId(), 0L)));
        }
        dataList.add(row);

        Map<String, Map<Integer, Long>> stormOffsetMap = kafkaInfoUtil.getStormOffset(topicMeta.topic());
        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "storm消费者的offset:" + (stormOffsetMap.isEmpty() ? "无" : ""));
        dataList.add(row);

        for (String group : stormOffsetMap.keySet()) {
            Map<Integer, Long> offsetMap = stormOffsetMap.get(group);
            if (offsetMap == null || offsetMap.isEmpty())
                continue;
            row = new HashMap<Object, String>();
            row.put(NAME_COLUMN, group);
            for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
                Long offset = offsetMap.get(meta.partitionId());
                offset = offset == null ? -1L : offset;
                row.put(meta.partitionId(), offset.toString());
            }
            dataList.add(row);
        }

        List<String> groupList = kafkaInfoUtil.getConsumerGroups();
        row = new HashMap<Object, String>();
        row.put(NAME_COLUMN, "消费者group的offset:" + (groupList.isEmpty() ? "无group" : ""));
        dataList.add(row);

        Collections.sort(groupList);
        for (String group : groupList) {
            row = new HashMap<Object, String>();
            row.put(NAME_COLUMN, group);
            for (PartitionMetadata meta : topicMeta.partitionsMetadata()) {
                String offset = kafkaInfoUtil.getConsumerOffset(group, topicMeta.topic(), meta.partitionId());
                offset = offset == null ? "-1" : offset;
                row.put(meta.partitionId(), offset);
            }
            dataList.add(row);
        }

        resultTable.getItems().setAll(dataList);

        resultTable.setVisible(true);
    }
}
