package com.github.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;

public class ConsumerDemoAssignSeek {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());

    public static void main(String[] args) {

        String bootstrap_servers ="localhost:9092";
        String topic ="first_topic";



        //Create Consumer Config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);

        //Create Consumer

        KafkaConsumer<String,String> consumer  = new KafkaConsumer<String, String>(props);

        //Consume data
        //assign and seek is are mostly used to replay data or fetch a specific message
        //assign

        TopicPartition topicPartitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(topicPartitionToReadFrom));

        //seek
        consumer.seek(topicPartitionToReadFrom,offsetToReadFrom);

        int numofMessagesToRead =5 ;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar =0;

        while(keepOnReading){
           ConsumerRecords<String,String> records= consumer
                   .poll(Duration.ofMillis(100));
           for(ConsumerRecord<String,String> consumerRecord:records){
               numberOfMessagesReadSoFar ++ ;
               logger.info("Key :"+consumerRecord.key());
               logger.info("Value :"+consumerRecord.value());
               logger.info("Offset :"+consumerRecord.offset());
               logger.info("Partition :"+consumerRecord.partition());
               logger.info("Timestamp :"+consumerRecord.timestamp());
               if(numberOfMessagesReadSoFar>=numofMessagesToRead){
                   keepOnReading = false;
                   break;

               }


           }
        }
    }
}
