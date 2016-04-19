package com.dangdang.tools.kafka.javafx;

import com.dangdang.tools.kafka.util.AlertUtil;
import com.dangdang.tools.kafka.util.KafkaInfoUtil;
import groovy.lang.GroovyClassLoader;
import groovy.lang.GroovyObject;
import groovy.lang.GroovyShell;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.scene.control.Label;
import javafx.scene.control.TabPane;
import javafx.scene.control.TextArea;
import javafx.scene.layout.FlowPane;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class KafkaMessageGrepGroovy extends KafkaMessageTail {
//    private GroovyShell groovyShell = new GroovyShell(getClass().getClassLoader());
    private GroovyClassLoader groovyClassLoader = new GroovyClassLoader(getClass().getClassLoader());
    private static final String INITIAL_FILTER_TEXT = "return String.valueOf(orderId).startsWith(\"33244\")";
    private String filterText = INITIAL_FILTER_TEXT;
    private Map<String, GroovyObject> groovyClassCacheMap = new HashMap<String, GroovyObject>();

    public KafkaMessageGrepGroovy(KafkaInfoUtil kafkaInfoUtil, TabPane tabPane) {
        super(kafkaInfoUtil, tabPane);
        this.setText("  tail -f | java json filter  ");
    }
    protected void addFilterControl(FlowPane pane) {
        final TextArea filterTextArea = new TextArea(INITIAL_FILTER_TEXT);
        filterTextArea.setPrefColumnCount(50);
        filterTextArea.setPrefRowCount(2);
        pane.getChildren().addAll(new Label("java表达式 (true时保留)："), filterTextArea);
        filterTextArea.focusedProperty().addListener(new ChangeListener<Boolean>() {
            @Override
            public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
                if (!newValue) { // 失去焦点
                    filterText = filterTextArea.getText().trim();
                }
            }
        });

        JavaFxUtil.addNewLine(pane);
        Label label = new Label("实际使用的是groovy表达式，兼容java语法，可以参数为: key、msg 消息的key、消息，" +
                "root 消息按json解析后的map，json中的各个key的名称都是可用的参数名。\n表达式中可以使用任何java语法，最后返回布尔值即可");
        label.setWrapText(true);
        pane.getChildren().add(label);
    }

    protected boolean shouldSkip(String key, String msg) {
        if (filterText.isEmpty())
            return false;

        long startTime = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append("class GroovyGrepFilter {");
        sb.append("  Boolean evaluate(String key, String msg, HashMap root");
        List<Object> variable = new ArrayList<Object>(30);
//        groovyShell.setProperty("key", key);
//        groovyShell.setProperty("msg", msg);
        try {
            HashMap root = new ObjectMapper().readValue(msg, HashMap.class);
            variable.add(key);
            variable.add(msg);
            variable.add(root);
//            groovyShell.setProperty("root", root);
            for (Object nodeKey : root.keySet()) {
                Object obj = root.get(nodeKey);
//                groovyShell.setProperty(nodeKey.toString(), obj);
                sb.append(", Object ").append(nodeKey.toString());
                variable.add(obj);
            }
        } catch (IOException e) {
            return true;
        }
        sb.append(") {\n").append(filterText).append("\n  }");
        sb.append("}");

//        Object result = groovyShell.evaluate(filterText);
        Object result = null;
        try {
            GroovyObject groovyObject = groovyClassCacheMap.get(sb.toString());
            if (null == groovyObject) {
                groovyObject = (GroovyObject) groovyClassLoader.parseClass(sb.toString()).newInstance();
                groovyClassCacheMap.put(sb.toString(), groovyObject);
            }
            result = groovyObject.invokeMethod("evaluate", variable.toArray());
        } catch (Exception e) {
//            System.out.println(sb.toString());
//            System.out.println(msg);
//            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
//        System.out.println((endTime - startTime) + " ms");
        if (result instanceof Boolean)
            return !(Boolean) result;
        if (result != null) {
            if ("true".equals(result.toString()))
                return false;
            if ("false".equals(result.toString()))
                return true;
        }

        return true; // 出错时skip
    }
}
