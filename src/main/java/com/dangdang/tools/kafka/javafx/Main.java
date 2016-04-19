package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import javafx.application.Application;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.EventHandler;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import javafx.stage.WindowEvent;

/**
 * Created by wangshichun on 2015/12/24.
 */
public class Main extends Application {
    private KafkaInfoUtil kafkaInfoUtil = new KafkaInfoUtil();

    @Override
    public void start(final Stage stage) throws Exception {
        // 窗口初始化
        stage.setTitle("kafka工具");
        final BorderPane borderPane = new BorderPane();
        final Scene scene = new Scene(borderPane, JavaFxUtil.screenSize.getWidth() / 2 + 50, 700);
        borderPane.setPrefWidth(scene.getWidth());
        stage.setScene(scene);
        stage.show();
        stage.setOnCloseRequest(new EventHandler<WindowEvent>() {
            @Override
            public void handle(WindowEvent event) {
                System.exit(0);
            }
        });

        // 默认样式
        setRootStyle(borderPane);

        // 上半部分
        final FlowPane head = new FlowPane();
        JavaFxUtil.setPreWidth(borderPane, new Region[] { head});
        setHead(head);
        borderPane.setTop(head);

        // 下半部分
        final TabPane tabPane = new TabPane();
        JavaFxUtil.setPreWidth(borderPane, new Region[] { tabPane});
        setTabs(tabPane);
        borderPane.setCenter(tabPane);

        borderPane.widthProperty().addListener(new ChangeListener<Number>() {
            @Override
            public void changed(ObservableValue<? extends Number> observable, Number oldValue, Number newValue) {
                JavaFxUtil.setPreWidth(borderPane, new Region[] { head, tabPane });
            }
        });
        JavaFxUtil.addWidthListener(borderPane, new Region[] { head, tabPane });
    }

    private void setHead(FlowPane pane) {
        Label label = new Label("请输入kafka对应的zookeeper地址，格式同kafka配置文件中的zookeeper.connect（例如 ）：");
        label.setStyle("-fx-padding: 0px;");
        pane.getChildren().add(label);
        pane.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + ";-fx-padding: 10px;");

        JavaFxUtil.addNewLine(pane);

        final TextField textField = new TextField("10.255.209.46:2181,10.255.209.47:2181/kafka");
        textField.setPrefColumnCount(30);
        pane.getChildren().add(textField);
        JavaFxUtil.addNewSpace(pane, 2);

        Button button = new Button("连接");
        pane.getChildren().add(button);
        button.setOnMouseClicked(new EventHandler<MouseEvent>() {
            @Override
            public void handle(MouseEvent event) {
                if (event.getClickCount() != 1)
                    return;
                kafkaInfoUtil.setKafkaZookeeperPath(textField.getText().trim());
            }
        });
    }

    private void setTabs(final TabPane tabPane) {
        tabPane.getTabs().addAll(new KafkaSummary(kafkaInfoUtil, tabPane),
                new KafkaMessageSummary(kafkaInfoUtil, tabPane),
                new KafkaMessageSend(kafkaInfoUtil, tabPane),
                new KafkaMessageTail(kafkaInfoUtil, tabPane),
                new KafkaMessageGrep(kafkaInfoUtil, tabPane),
                new KafkaMessageGrepGroovy(kafkaInfoUtil, tabPane));
    }

    private void setRootStyle(Pane pane) {
        pane.setStyle("-fx-background-color: " + JavaFxUtil.DEFAULT_BACKGROUND_COLOR + "; -fx-background-radius: 45px;");
    }

    public static void main(String[] args) {
        launch(args);
    }
}
