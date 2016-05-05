package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.beans.InvalidationListener;
import javafx.beans.Observable;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.geometry.Orientation;
import javafx.scene.control.*;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.FlowPane;
import javafx.scene.layout.Region;
import kafka.cluster.Broker;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import scala.collection.mutable.StringBuilder;

import java.util.*;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaSummary extends Tab {
//    private ListView<String> summaryResultList;
    private Accordion summaryResultAccordion;
    private KafkaInfoUtil kafkaInfoUtil;
    public KafkaSummary(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        this.setClosable(false);
        this.setText("  概要信息  ");
        this.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + ";");
        this.kafkaInfoUtil = kafkaInfoUtil;
        FlowPane pane = new FlowPane(Orientation.HORIZONTAL, 5, 5);
        JavaFxUtil.setPreWidth(tabPane, new Region[] { pane });
        JavaFxUtil.addWidthListener(tabPane, new Region[] { pane });
        pane.setStyle("-fx-padding: 5px;");
        this.setContent(pane);

        addGetSummaryButton(pane);
        JavaFxUtil.addNewLine(pane);
        addGetSummaryResultControl(pane);

        kafkaInfoUtil.doWhenConnected(new Runnable() {
            @Override
            public void run() {
                getSummaryAndDisplay();
            }
        });
    }

    private void addGetSummaryResultControl(final FlowPane pane) {
//        summaryResultList = new ListView<String>();
//        pane.getChildren().add(summaryResultList);
//        JavaFxUtil.setPreWidth(pane, new Region[] { summaryResultList });
//        JavaFxUtil.addWidthListener(pane, new Region[] { summaryResultList });

        summaryResultAccordion = new Accordion();
        summaryResultAccordion.setPrefWidth(JavaFxUtil.screenSize.getWidth() / 2);
        ScrollPane scrollPane = new ScrollPane(summaryResultAccordion);
        scrollPane.setPrefViewportHeight(500);
        scrollPane.setPrefViewportWidth(summaryResultAccordion.getPrefWidth() + 20);
        pane.getChildren().add(scrollPane);
    }

    private void addGetSummaryButton(FlowPane pane) {
        Button button = new Button("刷新概要信息");
        pane.getChildren().add(button);

        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;

                getSummaryAndDisplay();
            }
        });
    }

    private void getSummaryAndDisplay() {
        Map<Integer, Broker> brokerMap = kafkaInfoUtil.getAllBrokersInCluster();
        List<TopicMetadata> topicList = kafkaInfoUtil.getTopicMetadata(kafkaInfoUtil.getAllTopics());
        displayBrokersAndTopics(brokerMap, topicList);
    }
    private void displayBrokersAndTopics(Map<Integer, Broker> brokerMap, List<TopicMetadata> topicList) {
        int expandedIndex = summaryResultAccordion.getPanes().indexOf(summaryResultAccordion.getExpandedPane());
        summaryResultAccordion.getPanes().clear();
        StringBuilder sb = new StringBuilder();
        for (Integer key : brokerMap.keySet()) {
            Broker broker = brokerMap.get(key);
            sb.append(String.format("\tid: %s, host: %s, port: %s \n", broker.id(), broker.host(), broker.port()));
        }
        TextArea area = new TextArea(sb.substring(0, sb.length() - 1));
        area.setPrefRowCount(Math.min(brokerMap.size(), 15));
        area.setEditable(false);
        List<TitledPane> titledPaneList = new LinkedList<TitledPane>();
        TitledPane titledPane = new TitledPane("服务器信息", area);
        titledPane.setExpanded(true);
        titledPaneList.add(titledPane);

        Collections.sort(topicList, new Comparator<TopicMetadata>() {
            @Override
            public int compare(TopicMetadata o1, TopicMetadata o2) {
                return o1.topic().compareTo(o2.topic());
            }
        });

        for (TopicMetadata topic :  topicList) {
            if ("__consumer_offsets".equals(topic.topic()))
                continue;
            sb.clear();
            List<PartitionMetadata> partitionList = topic.partitionsMetadata();
            for (PartitionMetadata partition : partitionList) {
                sb.append(String.format("\tpartition index: %s\n", partition.partitionId()));
                sb.append(String.format("\tleader broker id: %s\n", null == partition.leader() ? "null" : partition.leader().id()));
                sb.append(String.format("\treplicas: %s\n", partition.replicas().toString()));
                sb.append(String.format("\tin sync replicas: %s\n", partition.isr().toString()));
                sb.append("\n");
            }
            area = new TextArea(sb.substring(0, sb.length() - 1));
            area.setPrefRowCount(Math.min(partitionList.size() * 5, 15));
            area.setEditable(false);
            titledPane = new TitledPane(String.format("topic: %s", topic.topic()), area);
            titledPane.setExpanded(true);
            titledPaneList.add(titledPane);
        }

        summaryResultAccordion.getPanes().addAll(titledPaneList);
        if (expandedIndex >= titledPaneList.size() || expandedIndex < 0)
            expandedIndex = 0;
        summaryResultAccordion.setExpandedPane(summaryResultAccordion.getPanes().get(expandedIndex));
//        summaryResultList.getItems().clear();
//        summaryResultList.getItems().addAll(sb.toString().split("\n"));
    }
}
