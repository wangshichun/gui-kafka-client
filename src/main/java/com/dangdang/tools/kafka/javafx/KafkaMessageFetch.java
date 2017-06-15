package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.AlertUtil;
import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.application.Platform;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.FlowPane;
import javafx.stage.FileChooser;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageFetch extends KafkaMessageTail {
    private Pattern filterPattern;

    public KafkaMessageFetch(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        super(kafkaInfoUtil, tabPane);
        this.setText(" fetch message ");
    }

    protected void addFilterControl(final FlowPane pane) {
        final TextField startOffsetTextField = new TextField("");
        final TextField endOffsetTextField = new TextField("");
        final String downloadText = "下载";
        final String stopText = "停止";
        final Button downloadButton = new Button(downloadText);
        final Button stopButton = new Button(stopText);

        pane.getChildren().addAll(new Label("start offset:  "), startOffsetTextField);
        JavaFxUtil.addNewSpace(pane, 2);
        pane.getChildren().addAll(new Label("end offset:   "), endOffsetTextField);
        JavaFxUtil.addNewSpace(pane, 4);
        pane.getChildren().addAll(downloadButton);
        JavaFxUtil.addNewSpace(pane, 2);
        pane.getChildren().addAll(stopButton);

        startOffsetTextField.setPrefColumnCount(15);
        endOffsetTextField.setPrefColumnCount(15);

        topicPartitionComboBox.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                String topic = topicComboBox.getSelectionModel().getSelectedItem();
                String partition = topicPartitionComboBox.getSelectionModel().getSelectedItem();
                if (topic == null || topic.isEmpty() || partition == null || partition.isEmpty() || !Character.isDigit(partition.charAt(0)))
                    return;

                TopicMetadata topicMeta = kafkaInfoUtil.getTopicMetadata(topic);
                for (PartitionMetadata partitionMetadata : topicMeta.partitionsMetadata()) {
                    if (partitionMetadata.partitionId() != Integer.parseInt(partition))
                        continue;
                    long[] offset = kafkaInfoUtil.getOffset(partitionMetadata, topic);
                    if (offset != null && offset.length == 2) {
                        startOffsetTextField.setText(String.valueOf(offset[1]));
                        endOffsetTextField.setText(String.valueOf(offset[0]));
                    }
                }
            }
        });

        final AtomicBoolean stopDownload = new AtomicBoolean(false);
        stopButton.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() == 1) {
                    stopDownload.set(true);
                }
            }
        });
        downloadButton.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                final String topic = topicComboBox.getSelectionModel().getSelectedItem();
                final String partition = topicPartitionComboBox.getSelectionModel().getSelectedItem();
                final long[] startOffset = {Long.parseLong(startOffsetTextField.getText())};
                final long endOffset = Long.parseLong(endOffsetTextField.getText());
                if (topic == null || topic.isEmpty() || partition == null || partition.isEmpty() || !Character.isDigit(partition.charAt(0))) {
                    AlertUtil.alert("请先选中topic和partition");
                    return;
                }
                FileChooser fileChooser = new FileChooser();
                fileChooser.setTitle("选择消息保存的文件名");
                fileChooser.setInitialDirectory(new File(System.getProperty("user.dir")));
                fileChooser.setInitialFileName("message-" + topic + "-" + partition + "__" + startOffset[0] + "-" + endOffset +  ".txt");
                fileChooser.getExtensionFilters().add(new FileChooser.ExtensionFilter("文本文件", "txt"));
                final File file = fileChooser.showSaveDialog(topicComboBox.getScene().getWindow());
                if (file == null) {
                    return;
                }
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        TopicMetadata topicMeta = kafkaInfoUtil.getTopicMetadata(topic);
                        if (null == topicMeta) {
                            AlertUtil.alert("请先选中topic和partition！");
                            return;
                        }
                        for (PartitionMetadata partitionMetadata : topicMeta.partitionsMetadata()) {
                            if (partitionMetadata.partitionId() != Integer.parseInt(partition))
                                continue;
                            BufferedWriter writer = null;
                            try {
                                writer = new BufferedWriter(new FileWriter(file.getAbsolutePath()));
                                stopDownload.set(false);

                                while (startOffset[0] < endOffset) {
                                    if (stopDownload.get())
                                        break;

                                    ByteBufferMessageSet messageSet = kafkaInfoUtil.fetchMessages(partitionMetadata, topic, startOffset[0]);
                                    if (messageSet == null || !messageSet.iterator().hasNext())
                                        break;

                                    for (MessageAndOffset messageAndOffset : messageSet) {
                                        String msg = KafkaInfoUtil.toString(messageAndOffset.message().payload());
                                        writer.write(msg);
                                        writer.write("\n");
                                        if (messageAndOffset.offset() >= startOffset[0])
                                            startOffset[0] = messageAndOffset.nextOffset();
                                    }
                                }
                                break;
                            } catch (Exception e) {
                                e.printStackTrace();
                                AlertUtil.alert("出错：" + e.getMessage());
                            } finally {
                                try {
                                    if (writer != null)
                                        writer.close();
                                    AlertUtil.alert(stopDownload.get() ? "已取消下载" : "下载完成");
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                    }
                }).start();
            }
        });
    }

    protected void addTailControl(FlowPane pane) {
    }
}
