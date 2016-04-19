package com.dangdang.tools.kafka.javafx;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.geometry.*;
import javafx.geometry.Insets;
import javafx.scene.control.Label;
import javafx.scene.layout.*;

import java.awt.*;
import java.util.Arrays;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class JavaFxUtil {
    public final static String DEFAULT_BACKGROUND_COLOR = "aliceblue";
    public static Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

    public static void addNewLine(FlowPane pane) {
        Pane line = new Pane();
        line.setStyle("-fx-padding: 0px; -fx-border-color: blue;");
        line.setPrefWidth(screenSize.getWidth());
        line.setPrefHeight(0);
        line.setVisible(false);
        pane.getChildren().add(line);
    }
    public static void addNewSpace(Pane pane, int spaceCount) {
        char[] spaces = new char[spaceCount];
        Arrays.fill(spaces, ' ');
        Label label = new Label(new String(spaces));
        label.setPrefHeight(0);
        label.setStyle("-fx-padding: 0px;");
        pane.getChildren().add(label);
    }
    public static void setPreWidth(Region parent, Region[] panes) {
        for (Region pane : panes) {
            pane.setPrefWidth(parent.getPrefWidth() - parent.getPadding().getLeft() * 2 - 5);
        }
    }
    public static void addWidthListener(final Region parent, final Region[] panes) {
        parent.prefWidthProperty().addListener(new ChangeListener<Number>() {
            @Override
            public void changed(ObservableValue<? extends Number> observable, Number oldValue, Number newValue) {
                JavaFxUtil.setPreWidth(parent, panes);
            }
        });
        parent.widthProperty().addListener(new ChangeListener<Number>() {
            @Override
            public void changed(ObservableValue<? extends Number> observable, Number oldValue, Number newValue) {
                JavaFxUtil.setPreWidth(parent, panes);
            }
        });
    }
}
