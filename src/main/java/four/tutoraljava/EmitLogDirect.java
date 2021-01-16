package four.tutoraljava;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/*
 *@Title RabbitMQ：Routing ：Receiving messages selectively
 *@Description Direct exchange:队列的binding key和消息的routing key完全相同，交换机
 * 就把这个消息发送到这个匹配到的队列
 * https://www.rabbitmq.com/tutorials/tutorial-four-java.html
 * https://github.com/rabbitmq/rabbitmq-tutorials/blob/master/java/EmitLogDirect.java
 *@Author bob.chen
 *@Date 2020/12/19 0019  下午 4:01
 *Version 1.0
*/
public class EmitLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            //声明direct交换机
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

            String severity = getSeverity(args);
            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME, severity, null, message.getBytes("UTF-8"));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");
        }
    }
    //获取日志严重性级别--error,info,waring
    private static String getSeverity(String[] arr){
        if(arr.length <1){
            return "info";
        }
        return arr[0];
    }

    private static String getMessage(String[] arr){
        if(arr.length <2){
            return "Hello World";
        }
        return  joinStrings(arr, " ", 1);
    }
    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length <= startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}
