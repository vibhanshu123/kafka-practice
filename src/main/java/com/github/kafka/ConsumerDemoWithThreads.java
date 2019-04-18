package com.github.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class ConsumerDemoWithThreads {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    public static void main(String[] args) {

        //Create Consumer
        new ConsumerDemoWithThreads().run();
    }

    private ConsumerDemoWithThreads(){

    }

    private void run(){
        String bootstrap_servers ="localhost:9092";
        String topic ="first_topic";
        String groupId="my-sixth-application";


        //Create Consumer Config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);
        CountDownLatch countDownLatch = new CountDownLatch(1);

        logger.info("Creating the Consumer Thread");
        //Create the Consumer Runnable
        Runnable myConsumer = new ConsumerThread(props,countDownLatch,topic);


        //Start the Thread
        Thread t = new Thread(myConsumer);
        t.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught Shutdwon Hook");
            ((ConsumerThread) myConsumer).shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        //latch for dealing with multiple threads
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application is Interrupted",e);

        }finally {
            logger.info("Application is Closing");
        }



    }

    public static class ConsumerThread implements Runnable{

        private Logger consumerlogger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        private CountDownLatch latch = null;
        private Properties props = null;
        private KafkaConsumer<String,String> consumer = null;
        private String topic = null;

        public ConsumerThread(Properties props , CountDownLatch latch, String topics){
           this.latch = latch;
           this.props = props;
           this.topic = topics;
        }

        @Override
        public void run() {
            consumer = new KafkaConsumer<String, String>(props);
            Collection<String> topics = Arrays.asList(topic);
            consumer.subscribe(topics);
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer
                            .poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> consumerRecord : records) {
                        logger.info("Key :" + consumerRecord.key());
                        logger.info("Value :" + consumerRecord.value());
                        logger.info("Offset :" + consumerRecord.offset());
                        logger.info("Partition :" + consumerRecord.partition());
                        logger.info("Timestamp :" + consumerRecord.timestamp());


                    }
                }
            } catch (WakeupException e) {
                consumerlogger.error("Received Shutdown Signal");
            } finally{
                consumer.close();
                latch.countDown();
            }
        }


        public void shutdown(){

            //The wakeup method is a special method to interrupt consumer.poll .It will
            //Throw the exception WakeUpEception
            consumer.wakeup();
        }
    }
}


