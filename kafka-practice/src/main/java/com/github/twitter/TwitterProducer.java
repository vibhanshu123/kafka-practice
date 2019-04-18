package com.github.twitter;

import com.google.common.collect.*;
import com.twitter.hbc.*;
import com.twitter.hbc.core.*;
import com.twitter.hbc.core.endpoint.*;
import com.twitter.hbc.core.processor.*;
import com.twitter.hbc.httpclient.auth.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;

public class TwitterProducer {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private final String consumerKey = "G5ObQsB2gUGGZDnQhrJVQRGYB";
    private final String consumerSecret = "bhFVXLha46LQwPCcyJN9okNJWa15O2Tjhtyyl70EtsZfLy3ckY";
    private final String token = "136541817-yDwu8zZm0XayhASW4QXNuUYL3lsq2PkCCZEAOSPt";
    private final String secret = "4wx8U5oLCMcLjJmcQ1znBTj8XWZWAMnxpZ1wMittpYqji";
    private final String bootstrap_servers = "localhost:9092";
    private final String topic = "twitter_tweets";
    // Optional: set up some followings and track terms
    //    List<Long> followings = Lists.newArrayList(1234L, 566788L);
    private List<String> terms = Lists.newArrayList("manjari","politics","election");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("SET UP");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //     BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
        //CREATE  A TWITTER CLIENT
        Client client = createTwitterClient(msgQueue);
        //Attempts to establish a connection
        client.connect();

        //CREATE A KAFKA PRODUCER
        KafkaProducer<String, String> producer = createKafkaProducer();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
         logger.info("Stopping Application");
         logger.info("Shutting down client from twitter");
         client.stop();
         logger.info("closing producer....");
         producer.close();
         logger.info("DONE");
        }));

        //LOOP TO SEND TWEEETS TO KAFKA

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Received New Metadata. \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() + "----" +
                                    recordMetadata.toString());
                        } else {
                            logger.error("Error While Producing", e);
                        }
                    }
                });
            }

        }

    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //creater producer properties

        Properties props = new Properties();
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);


        //Create Safe producer
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all"); //(Deafult is 1 if you set idempotence producer)
        props.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));//Default
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");//KAFKA 2.0>=1.1SO
        //WE CAN KEEP THIS AS 5 , USE 1 OTHERWISE Default

        //high throughput producer at the expense of a bit of latency and CPU usage
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(1024*32));



        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);
        return kafkaProducer;
    }

    //The link is :-https://github.com/twitter/hbc
    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        //     hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        //  Creating a client:

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        // .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        return hosebirdClient;
    }

}
