package com.github.kafkastreams;

import com.google.gson.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.*;

public class StreamsFilterTweets {

    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) {
        //Create Properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //Create a Topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStreams = inputTopic.filter(
          //filter for tweets which has user of more than 10000 followers
                (k,jsonTweet)->extractUserFollowersFromTweet(jsonTweet)>10000);
        filteredStreams.to("important_tweets");


        //Build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),props);
        //Start our streams application
        kafkaStreams.start();

    }









    private static Integer extractUserFollowersFromTweet(String value) {
        return  jsonParser.parse(value).getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();
    }
}
