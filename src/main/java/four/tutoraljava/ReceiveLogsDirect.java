package four.tutoraljava;

import com.rabbitmq.client.*;

/*
 *@Title Routing--消费者代码
 *@Description 官网Direct案例
 *@Author bob.chen
 *@Date 2020/12/19 0019  下午 4:18
 *Version 1.0
*/
public class ReceiveLogsDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] argv) throws  Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //声明direct类型的exchange
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        //获取随机生成的队列名
        String queueName = channel.queueDeclare().getQueue();

        if (argv.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        for(String severity : argv){
            channel.queueBind(queueName,EXCHANGE_NAME,severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = ((consumerTag, delivery) -> {
           String msg = new String(delivery.getBody(),"UTF-8");
           System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + msg + "'");
        });
        channel.basicConsume(queueName,true,deliverCallback,consumerTag ->{
            System.out.println("consumerTag:"+consumerTag);
        });
    }
}
