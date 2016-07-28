# gui-kafka-client
kafka client gui tool for: info summary, message count, comsumer comsume offset view, storm-kafka offset view, send message or download or "tail -f" message


使用方法：运行target目录中的jar文件即可。


+ JDK 1.8

相关界面：
统计各分区中的消息数、起止offset、下载各分区中的消息内容。

<img src="https://raw.githubusercontent.com/wangshichun/gui-kafka-client/master/src/main/resources/kafka_message_count.png"/>

使用正则表达式过滤并显示kafka中的消息内容。

<img src="https://raw.githubusercontent.com/wangshichun/gui-kafka-client/master/src/main/resources/kafka_message_consume.png"/>


向kafka中发送消息。

<img src="https://raw.githubusercontent.com/wangshichun/gui-kafka-client/master/src/main/resources/kafka_message_send.png"/>
