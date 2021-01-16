package three.tutorialjava;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/*
 *@Title TODO
 *@Description TODO
 *@Author bob.chen
 *@Date 2020/12/18 0018  上午 12:14
 *Version 1.0
*/
public class ReceiveLogs {
    private static final String  EXCHANGE_NAME="log";

    public static void main(String[] args) throws  Exception {
        ConnectionFactory  factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //声明交互机类型，交换机名称
        channel.exchangeDeclare(EXCHANGE_NAME,"fanout");
       //获取一个随机生成的队列名
        String queueName = channel.queueDeclare().getQueue();
        //绑定队列和交换机
        channel.queueBind(queueName,EXCHANGE_NAME,"");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
        DeliverCallback deliverCallback =(consumerTag,delivery)->{
           String msg = new String(delivery.getBody(),"UTF-8");
            System.out.println(" [x] Received '" + msg + "'");
        };
        channel.basicConsume(queueName,true,deliverCallback,consumerTag ->{} );

    }
}
