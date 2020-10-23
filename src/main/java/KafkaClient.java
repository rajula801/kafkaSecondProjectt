import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;


public class KafkaClient {


    public KafkaClient(){

    }
    public static void main(String args[]){
        new KafkaClient().callClient();
    }

    String consumerKey = "hLOS371OYbdSeeFC5pCgLn2W6";
    String consumerSecret = "taJQOc95cPAxFcv6mzQiSYOG7vYmYrBqc4uz0TXk4wdxcLINx5";
    String token = "934983867504750592-jxP3Udf3rSjcn1wlBk4XAJ4LeCB6271";
    String secret = "rwbGXU5Yd8LlRA60Y5nTDpvb4q2nq2ujyqqviPga0KTLI";
    List<String> terms = Lists.newArrayList("cashaa");
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

    public void callClient() {
        System.out.println("class begins");
        Client client = createTwitterClent(msgQueue);
        client.connect();
        final KafkaProducer<String,String> producer = createproducer();


        Runtime.getRuntime().addShutdownHook(new Thread( ()-> {
            System.out.println("closing the client");
            client.stop();
            System.out.println("closing the producer");
            producer.close();
            System.out.println("done");

        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
                System.out.println("class begins");
            }
            if(msg != null){
                System.out.println("Message  : "+msg);
                producer.send(new ProducerRecord("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                            System.out.println("Something went wrong");
                        }
                    }
                });
            }
        }
        System.out.println("End of class");


    }

    public Client createTwitterClent(BlockingQueue<String> msgQueue) {
        BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);
// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));


        Client hosebirdClient = builder.build();
// Attempts to establish a connection.
        //hosebirdClient.connect();
        return hosebirdClient;

    }
 public KafkaProducer<String,String> createproducer(){

        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
     properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
     KafkaProducer<String,String>  producer = new KafkaProducer<String, String>(properties);
     return producer;
 }


}