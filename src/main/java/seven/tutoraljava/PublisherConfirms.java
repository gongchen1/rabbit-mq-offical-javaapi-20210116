package seven.tutoraljava;

import com.rabbitmq.client.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.time.Duration;
import java.util.UUID;

/*
 *@Title publisher confirm ：确保消息可靠的发送到broker
 *@Description .RabbitMQ官网教程--自己研究
 *@Author bob.chen
 *@Date 2021/1/16 0016  下午 10:41
 *Version 1.0
*/
public class PublisherConfirms {
    static final int MESSAGE_COUNT = 50_000;

    public static void main(String[] args) throws  Exception {
        //单独发送一条消息，同步等待broker确认
        publishMessagesIndividually();
    }






    /**
     * publisher confirm :单独发送一条消息，同步等待broker确认收到这个消息，并反馈给客户端
     * @throws Exception
     */
    public static void publishMessagesIndividually() throws  Exception{
        try(Connection connection = createConnection()) {
            Channel channel = connection.createChannel();
            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue,false,false,true,null);

            //开启publisher confirm机制
            channel.confirmSelect();
            long start = System.nanoTime();
            for(int i=0; i< MESSAGE_COUNT;i++){
                byte[] body = String.valueOf(i).getBytes();
                channel.basicPublish("",queue,null,body);

                //等待所有被发送的消息，知道最后一个消息被确认或者未被确认
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }

    }


    public static Connection  createConnection() throws  Exception{
        ConnectionFactory  connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        return  connectionFactory.newConnection();
    }

}
