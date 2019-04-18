package com.github.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import java.util.*;

public class ProducerDemo {

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

        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>(topic,"Hello World");

        //send data --asynchronous
        kafkaProducer.send(producerRecord);

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
