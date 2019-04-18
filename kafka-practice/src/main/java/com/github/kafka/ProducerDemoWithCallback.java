package com.github.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;

public class ProducerDemoWithCallback {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        String bootstrap_servers ="localhost:9092";
        String topic ="first_topic";

        //creater producer properties

        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);

        //Create producer

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(props);

        //Create a Producer Record

        for(int i=0;i<10;i++) {

            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, "Hello World"+i);

            //send data --asynchronous
            kafkaProducer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received New Metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "----" + recordMetadata.toString());
                    } else {
                        logger.error("Error While Producing", e);
                    }
                }
            });
        }
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
