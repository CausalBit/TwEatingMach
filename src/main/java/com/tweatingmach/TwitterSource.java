package com.tweatingmach;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import com.twitter.hbc.twitter4j.handler.StatusStreamHandler;
import com.twitter.hbc.twitter4j.message.DisconnectMessage;
import com.twitter.hbc.twitter4j.message.StallWarningMessage;

import twitter4j.StatusListener;
import twitter4j.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by irvin on 6/3/17.
 */
public class TwitterSource implements java.io.Serializable {

    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;
    private boolean isStreaming;

    private BlockingQueue<String> queue;
    private StatusListener statusListener;
    private List<StatusListener> listeners;
    private transient  Twitter4jStatusClient client;
    private transient BasicClient  clientBasic;



    public TwitterSource(){
        listeners = new ArrayList<StatusListener>();
        loadLoginCredentials();
        isStreaming = true;
        queue = new LinkedBlockingQueue<String>(10000);


    }

    public boolean isDoneBasic(){
        return clientBasic.isDone();

    }

    public void stopStreaming(){
        clientBasic.stop();
    }


    public void streamData(List<String> filterTerms, List<Location> locations ) throws InterruptedException{

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.stallWarnings(false);  //Avoid pulling the stalling warnings.


        /**TRACKING TERMS
         * The text of the Tweet and some entity fields are considered for matches.
         * Specifically, the text attribute of the Tweet, expanded_url and display_url
         * for links and media, text for hashtags, and screen_name for user mentions are checked for matches.
         */
        endpoint.trackTerms(filterTerms);


        /**LOCATION
         A comma-separated list of longitude,latitude pairs specifying a set of bounding boxes to filter
         Tweets by. Only geolocated Tweets falling within the requested bounding boxes will be
         included—unlike the Search API, the user’s location field is not used to filter Tweets.

         Each bounding box should be specified as a pair of longitude and latitude pairs, with the
         southwest corner of the bounding box coming first. For example:
         -122.75,36.8,-121.75,37.8 	-> San Francisco
         */
        Location.Coordinate southWestCoordinate = new Location.Coordinate(7.8,-86.5);
        Location.Coordinate northEastCoordinate = new Location.Coordinate( 11.4,-82.4);
        Location CostaRica = new Location(southWestCoordinate, northEastCoordinate);

        //We can actually make a list of locations to pass it into trackLocations...
        //Lets use the example, instead of the one from the arguments.
        List<Location> locationsExample = new ArrayList<Location>();
        //locationsExample.add(CostaRica);

        //endpoint.locations(locationsExample);

        //This is to access the tweets.
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);


        clientBasic = new ClientBuilder()
                .name("TEMClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        //Start the connection.
        clientBasic.connect();

    }


    public void readStream(){

        while(!clientBasic.isDone()){
            try {
                if(!queue.isEmpty() ) {
                    String message = queue.take();
                    System.out.println(message);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public synchronized  String getTweet(){
        try {
            String valor = queue.poll(1L,TimeUnit.SECONDS);
            if( valor==null ){
               safePrintln("Waiting for tweet...");
                return "NO-TWEET";
            }
            return valor;
        }catch(Exception e){
            e.printStackTrace();
            stopStreaming();
            System.exit(1);
            return "NO-TWEET"; //TODO handle this exception gracefully like swan, but fiercely like a lioness.
        }
    }


    public void loadLoginCredentials(){
       JSONParser parser = new JSONParser();
        try {

            Object obj = parser.parse(new FileReader("./src/main/java/com/tweatingmach/twitterLogin.txt"));
            JSONObject jsonObject = (JSONObject) obj;
            consumerKey = (String) jsonObject.get("consumerKey");
            consumerSecret = (String) jsonObject.get("consumerSecret");
            token = (String) jsonObject.get("token");
            secret = (String) jsonObject.get("secret");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void safePrintln(String s) {
        synchronized (System.out) {
            System.out.println(s);
        }
    }

    public void streamDataWithThreads(List<String> filterTerms, List<Location> locations ) throws InterruptedException{


        //Use this end point to receive the tweets.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.stallWarnings(false);  //Avoid pulling the stalling warnings.


        /**TRACKING TERMS
         * The text of the Tweet and some entity fields are considered for matches.
         * Specifically, the text attribute of the Tweet, expanded_url and display_url
         * for links and media, text for hashtags, and screen_name for user mentions are checked for matches.
         */
        endpoint.trackTerms(filterTerms);


        /**LOCATION
         A comma-separated list of longitude,latitude pairs specifying a set of bounding boxes to filter
         Tweets by. Only geolocated Tweets falling within the requested bounding boxes will be
         included—unlike the Search API, the user’s location field is not used to filter Tweets.

         Each bounding box should be specified as a pair of longitude and latitude pairs, with the
         southwest corner of the bounding box coming first. For example:
         -122.75,36.8,-121.75,37.8 	-> San Francisco
         */
        Location.Coordinate southWestCoordinate = new Location.Coordinate(-122.75,36.8);
        Location.Coordinate northEastCoordinate = new Location.Coordinate(-121.75,37.8);
        Location sanFrancisco = new Location(southWestCoordinate, northEastCoordinate);

        //We can actually make a list of locations to pass it into trackLocations...
        //Lets use the example, instead of the one from the arguments.
        List<Location> locationsExample = new ArrayList<Location>();
        locationsExample.add(sanFrancisco);

        //endpoint.locations(locationsExample);

        //This is to access the tweets.
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);


        Client pre_client = new ClientBuilder()
                .name("TEMClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        setupListeners();
        try {
            listeners.add(statusListener);
        }catch(Exception e){
            e.printStackTrace();
        }

        // Service that will set up the threads to parse the incoming
        // messages (calling the listeners on each message).
        int numThreads = 4;
        ExecutorService service = Executors.newFixedThreadPool(numThreads);

        client = new Twitter4jStatusClient(pre_client, queue, listeners, service);
        client.connect(); //Get the connection.

        for(int i = 1 ; i < numThreads; i++){
            client.process(); //Begin processing messages on each thread;
        }

    }

    private void setupListeners(){
        statusListener = new StatusStreamHandler(){

            @Override
            public void onDisconnectMessage(DisconnectMessage message){
                System.out.println("Connection lost");
            }

            @Override
            public void onException(Exception e){
                e.getMessage();
                e.printStackTrace();
            }

            @Override
            public void onStallWarningMessage(StallWarningMessage warning){
                System.out.println("stall lost");
            }

            @Override
            public void onUnknownMessageType(String msg){
                System.out.println("unknow message typet");
            }

            @Override
            public void onTrackLimitationNotice( int x){}

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice){

            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId){

            }

            @Override
            public void onStatus(Status status){
                System.out.println("status "+status+"\n\n");

                try {
                    queue.put("Dummy I am lost");
                }catch(InterruptedException e){
                    System.out.println("Waiting to put in");
                }
            }

            @Override
            public void onStallWarning(StallWarning warning){

            }

        };
    }

    public void stopStreamingWithThreads(){
        client.stop(); //Stops the client and the executor service.
    }
}
