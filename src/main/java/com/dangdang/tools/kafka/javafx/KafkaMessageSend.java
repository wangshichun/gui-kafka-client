package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.geometry.Orientation;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Region;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;

import java.util.*;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageSend extends Tab {
    private KafkaInfoUtil kafkaInfoUtil;
    private ComboBox<String> topicComboBox;
    private ComboBox<String> topicPartitionComboBox;
    private TextField messageKeyTextField;
    private TextArea messageValueTextField;

    public KafkaMessageSend(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        this.setClosable(false);
        this.setText("发送消息producer");
        this.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + ";");
        this.kafkaInfoUtil = kafkaInfoUtil;
        FlowPane pane = new FlowPane(Orientation.HORIZONTAL, 5, 5);
        JavaFxUtil.setPreWidth(tabPane, new Region[] { pane });
        JavaFxUtil.addWidthListener(tabPane, new Region[] { pane });
        pane.setStyle("-fx-padding: 5px;");
        this.setContent(pane);

        addTopicComboBox(pane);
        JavaFxUtil.addNewSpace(pane, 2);
        addGetAllTopicsButton(pane);
        JavaFxUtil.addNewLine(pane);
        addKeyTextfield(pane);
        JavaFxUtil.addNewLine(pane);
        addValueTextarea(pane);
        JavaFxUtil.addNewLine(pane);
        addSendMessageButton(pane);


//        getTopicsAndDisplay();
        kafkaInfoUtil.doWhenConnected(new Runnable() {
            @Override
            public void run() {
                getTopicsAndDisplay();
            }
        });
    }

    private void addSendMessageButton(FlowPane pane) {
        Button button = new Button("发送消息");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                String topic = topicComboBox.getSelectionModel().getSelectedItem();
                Integer partition = null;
                try {
                    partition = Integer.valueOf(topicPartitionComboBox.getSelectionModel().getSelectedItem());
                } catch (Exception e) {
                    partition = null;
                }
                String key = messageKeyTextField.getText();
                key = key.isEmpty() ? null : key;
                String value = messageValueTextField.getText();
                kafkaInfoUtil.sendMessage(topic, partition, key, value);
            }
        });
    }

    private void addValueTextarea(FlowPane pane) {
        messageValueTextField = new TextArea();
        messageValueTextField.setWrapText(true);
        pane.getChildren().addAll(new Label("消息value："), messageValueTextField);
    }

    private void addKeyTextfield(FlowPane pane) {
        messageKeyTextField = new TextField();
        messageKeyTextField.setPrefColumnCount(30);
        pane.getChildren().addAll(new Label("消息key："), messageKeyTextField);
    }

    private void addTopicComboBox(FlowPane pane) {
        topicComboBox = new ComboBox<String>();
        pane.getChildren().addAll(new Label("topic："), topicComboBox);
        JavaFxUtil.addNewLine(pane);

        topicPartitionComboBox = new ComboBox<String>();
        pane.getChildren().addAll(new Label("分区："), topicPartitionComboBox);
        topicComboBox.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                getTopicPartitionsAndDisplay(topicComboBox.getSelectionModel().getSelectedItem());
            }
        });
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
        partitionList.add("自动根据key选择分区，无key时随机");
        for (PartitionMetadata partitionMetadata : meta.partitionsMetadata()) {
            partitionList.add(String.valueOf(partitionMetadata.partitionId()));
        }
        topicPartitionComboBox.getItems().addAll(partitionList);
        if (null != par)
            topicPartitionComboBox.getSelectionModel().select(par);
        if (topicPartitionComboBox.getSelectionModel().getSelectedIndex() < 0)
            topicPartitionComboBox.getSelectionModel().select(0);
    }

    private void addGetAllTopicsButton(FlowPane pane) {
        Button button = new Button("刷新");
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
