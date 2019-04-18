package com.github.elasticsearch;

import com.github.kafka.*;
import com.google.gson.*;
import jdk.nashorn.internal.parser.*;
import org.apache.http.*;
import org.apache.http.auth.*;
import org.apache.http.client.*;
import org.apache.http.impl.client.*;
import org.apache.http.impl.nio.client.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;
import org.elasticsearch.*;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.index.*;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.builder.*;
import org.slf4j.*;

import java.io.*;
import java.time.*;
import java.util.*;

public class ElasticsearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());

    private final String bootstrap_servers ="localhost:9092";
    private final String topic ="twitter_tweets";
    private final String groupId="twitter-consumer-elasticsearch2";
    private final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws IOException{
        new ElasticsearchConsumer().run();
    }

    public void run() throws IOException {

        //Create Kafka Consumer
        KafkaConsumer<String,String> consumer= getKafkaConsumer(topic);

        //Get  Elastic search cluster rest client
        RestHighLevelClient client = getRestClient();

       String jsonString="{\"foo\":\"bar\"}";

        //Consume data and push it to elastic search cluster

        while(true){
            ConsumerRecords<String,String> records= consumer
                    .poll(Duration.ofMillis(100));
            int recordsCount = records.count();
            logger.info("Received:-"+recordsCount+" records ");
            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String,String> consumerRecord:records){
             //   logger.info("Offset :"+consumerRecord.offset());

                //2 strategies for creating ids
                //kafka generic id
                String id =consumerRecord.topic()+"_"+consumerRecord.partition()+"_"+
                        consumerRecord.offset();

                //twitter feed specific id
                String id1 = extractIdFromTweet(consumerRecord.value());

                IndexRequest indexRequest = new IndexRequest(
                        "twitter",
                        "tweets",
                        id1) //This is to make the consumer idempotent
                        .source(consumerRecord.value(),XContentType.JSON);
                bulkRequest.add(indexRequest);

            }
            if(recordsCount>0) {
                BulkResponse response = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets Have been committed..");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
        //close the client gracefully
        // client.close();
    }



    private String extractIdFromTweet(String value) {
       return  jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
    }


    public RestHighLevelClient getRestClient(){
        String hostname="twitter-kafka-app-5366108588.ap-southeast-2.bonsaisearch.net";
        String username="72z7sncb9g";
        String password="imwjenjwx0";

        //Now only for bonsai/cloud you need to create Credentials Provider object
       final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
       credentialsProvider.setCredentials(AuthScope.ANY,new UsernamePasswordCredentials(username,password));

       //RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
       //        new HttpHost("localhost", 9200, "http")));

       RestClientBuilder builder = RestClient
               .builder(new HttpHost(hostname,443,"https"))
               .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                   @Override
                   public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                       return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                   }
               });

       RestHighLevelClient client= new RestHighLevelClient(builder);
       return client;
   }





    public KafkaConsumer<String,String> getKafkaConsumer(String topics){
        //Create Consumer Config
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//disable auto commit of offsets
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_servers);


        //Create Consumer
        Collection<String> topicList = Arrays.asList(topics);
        KafkaConsumer<String,String> consumer  = new KafkaConsumer<String, String>(props);
        consumer.subscribe(topicList);
        return consumer;
    }
}
