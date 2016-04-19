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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageGrep extends KafkaMessageTail {
    private Pattern filterPattern;
    public KafkaMessageGrep(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        super(kafkaInfoUtil, tabPane);
        this.setText("  tail -f | grep  ");
    }
    protected void addFilterControl(FlowPane pane) {
        final TextArea filterText = new TextArea(".*\"orderId\":33244.+");
        filterText.setPrefColumnCount(50);
        filterText.setPrefRowCount(2);
        pane.getChildren().addAll(new Label("java正则表达式："), filterText);
        filterText.textProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
                setFilterPattern(filterText.getText().trim());
            }
        });
        setFilterPattern(filterText.getText().trim());
    }

    private void setFilterPattern(String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            filterPattern = null;
        }

        filterPattern = Pattern.compile(pattern, Pattern.DOTALL);
    }

    protected boolean shouldSkip(String key, String msg) {
        if (filterPattern == null)
            return false;

        return !filterPattern.matcher(msg).matches();
    }
}
