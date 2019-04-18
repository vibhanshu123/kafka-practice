package com.github.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.time.*;
import java.util.*;

public class ConsumerDemo {

    private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {

        String bootstrap_servers ="localhost:9092";
        String topic ="first_topic";
        String groupId="my-fourth-application";


        //Create Consumer Config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);

        //Create Consumer

        KafkaConsumer<String,String> consumer  = new KafkaConsumer<String, String>(props);

        Collection<String> topics = Arrays.asList(topic);

        //Consume data

        consumer.subscribe(topics);

        while(true){
           ConsumerRecords<String,String> records= consumer
                   .poll(Duration.ofMillis(100));
           for(ConsumerRecord<String,String> consumerRecord:records){
               logger.info("Key :"+consumerRecord.key());
               logger.info("Value :"+consumerRecord.value());
               logger.info("Offset :"+consumerRecord.offset());
               logger.info("Key :"+consumerRecord.key());
               logger.info("Key :"+consumerRecord.key());


           }
        }
    }
}
