package com.dangdang.tools.kafka.util;

import javafx.scene.control.Alert;
import javafx.scene.control.ButtonType;

import javax.swing.*;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Created by wangshichun on 2015/12/25.
 */
public class AlertUtil {
    public static void alert(String message) {
        alert(message, null);
    }
    public static void alert(String message, String title) {
        try {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setContentText(message);
            if (title != null)
                alert.setTitle(title);
            alert.showAndWait();
        } catch (Throwable e) {
            JOptionPane.showMessageDialog(null, message, title, JOptionPane.INFORMATION_MESSAGE);
        }
    }
    public static void alertNoWait(String message) {
        try {
            Alert alert = new Alert(Alert.AlertType.INFORMATION);
            alert.setContentText(message);
            alert.show();
        } catch (Throwable e) {
            JOptionPane.showMessageDialog(null, message, null, JOptionPane.INFORMATION_MESSAGE);
        }
    }

    public static boolean confirm(String message) {
        return confirm(message, null);
    }
    public static boolean confirm(String message, String title) {
        try {
            Alert alert = new Alert(Alert.AlertType.CONFIRMATION);
            alert.setContentText(message);
            if (title != null)
                alert.setTitle(title);
            Optional<ButtonType> buttonTypeOptional = alert.showAndWait();
            return buttonTypeOptional.orElseGet(new Supplier<ButtonType>() {
                @Override
                public ButtonType get() {
                    return ButtonType.CANCEL;
                }
            }).equals(ButtonType.OK);
        } catch (Throwable e) {
            return JOptionPane.YES_OPTION == JOptionPane.showConfirmDialog(null, message, title, JOptionPane.YES_NO_CANCEL_OPTION);
        }
    }
}
