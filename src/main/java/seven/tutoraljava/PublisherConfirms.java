package seven.tutoraljava;

import com.rabbitmq.client.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BooleanSupplier;

/*
 *@Title publisher confirm ：确保消息可靠的发送到broker
 *@Description  RabbitMQ官网教程--自己研究
 *@Author bob.chen
 *@Date 2021/1/16 0016  下午 10:41
 *Version 1.0
*/
public class PublisherConfirms {
    static final int MESSAGE_COUNT = 50_000;

    public static void main(String[] args) throws  Exception {
        //单独发送一条消息，同步等待broker确认
        //publishMessagesIndividually();

        //发送多条消息，批量等待被broker确认
        publishMessagesInBatch();
    }


    /**
     * publisher confirm :单独发送一条消息，同步等待broker确认收到这个消息，并反馈给客户端
     * 特点：简单，但是由于是同步等待消息服务器响应，所以会严重影响吞吐量
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
                //表示使用默认的交换机类型
                channel.basicPublish("",queue,null,body);

                //等待所有被发送的消息被服务器接收后的响应信息：直到最后一个消息被确认或者未被确认
                channel.waitForConfirmsOrDie(5_000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages individually in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }


    }

    /**
     * 发送多条消息，批量等待消息确认，直到最后一条消息被确认或者被丢失
     * 特点：简单、合理的吞吐量，但很难知道什么时候出错
     * @throws Exception
     */
    static void publishMessagesInBatch() throws Exception{
        try(Connection connection = createConnection()){
            Channel channel = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            channel.queueDeclare(queue,false,false,true,null);
            channel.confirmSelect();

            int batchSize = 100;
            int outstandingMessageCount = 0;

            long start = System.nanoTime();
            for(int i=0;i<MESSAGE_COUNT;i++){
                String body = String.valueOf(i);
                channel.basicPublish("",queue,null,body.getBytes());
                outstandingMessageCount++;
                //批量确认
                if(outstandingMessageCount==batchSize){
                    channel.waitForConfirmsOrDie(5000);
                    outstandingMessageCount=0;
                }

            }
            //说明有些没有发送成功，那么再继续等在这些确认
            if(outstandingMessageCount>0){
                channel.waitForConfirmsOrDie(5000);
            }
            long end = System.nanoTime();
            System.out.format("Published %,d messages in batch in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());
        }


    }

    /**
     * outstanding: 未完成的
     * @throws Exception
     */
    static void handlePublishConfirmsAsynchronously() throws Exception{
        try (Connection connection = createConnection()) {
            Channel ch = connection.createChannel();

            String queue = UUID.randomUUID().toString();
            ch.queueDeclare(queue, false, false, true, null);

            ch.confirmSelect();

            ConcurrentNavigableMap<Long,String>  outstandingConfirms = new ConcurrentSkipListMap<>();

            ConfirmCallback cleanOutstandingConfirms =(sequenceNumber, multiple)->{
              if(multiple){
                  ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(
                          sequenceNumber, true
                  );
                  confirmed.clear();
              }else{
                  outstandingConfirms.remove(sequenceNumber);
              }
            };
            //@param ackCallback callback on ack
            //@param nackCallback call on nack (negative ack):否定应答回调，未被broker确认的消息处理
            ch.addConfirmListener(cleanOutstandingConfirms,(sequenceNumber, multiple)->{
                String body = outstandingConfirms.get(sequenceNumber);
                System.err.format(
                        "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                        body, sequenceNumber, multiple
                );
                cleanOutstandingConfirms.handle(sequenceNumber,multiple);

            });
            long start = System.nanoTime();
            for(int i=0;i<MESSAGE_COUNT;i++){
                String body = String.valueOf(i);
                outstandingConfirms.put(ch.getNextPublishSeqNo(),body);
                ch.basicPublish("",queue,null,body.getBytes());
                if (!waitUntil(Duration.ofSeconds(60), () -> outstandingConfirms.isEmpty())) {
                    throw new IllegalStateException("All messages could not be confirmed in 60 seconds");
                }

                long end = System.nanoTime();
                System.out.format("Published %,d messages and handled confirms asynchronously in %,d ms%n", MESSAGE_COUNT, Duration.ofNanos(end - start).toMillis());

            }

        }
    }
    public static Connection  createConnection() throws  Exception{
        ConnectionFactory  connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        return  connectionFactory.newConnection();
    }
    static boolean waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        int waited = 0;
        while (!condition.getAsBoolean() && waited < timeout.toMillis()) {
            Thread.sleep(100L);
            waited = +100;
        }
        return condition.getAsBoolean();
    }

}
