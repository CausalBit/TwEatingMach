package com.tweatingmach;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.commons.io.FileUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
/**
 * Created by irvin on 6/3/17.
 */
public class TwitterSource {
    private String consumerKey;
    private String consumerSecret;
    private String token;
    private String secret;

    private boolean keepStreaming;
    BlockingQueue<String> queue;

    public TwitterSource(){
        /*
        consumerKey = "2ylE4rRu1gjQ2bPP598FRJ3Hy";
        consumerSecret = "PT50AmclEIfeP2GzZISgYglkvdG45YGCKTdLJrpcFdoAdzvtI7";
        token = "871044421994905605-dL889t3gt0n8HY5oei1tP709ddYaZP1";
        secret = "BCmhDDVvrVad7LzDcQn5YrlCDmzSRNps4yX5NJ2E13OQh";*/
        updateLogin();

        keepStreaming = true;
        queue = new LinkedBlockingQueue<String>(10000);

    }

    public void Feed(List<String> filterTerms, List<Location> locations ) throws InterruptedException{


        //Use this end point to receive the tweets.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.stallWarnings(false);  //Avoid pulling the stalling warnings.


        /**TRACKING TERMS
         * The text of the Tweet and some entity fields are considered for matches.
         * Specifically, the text attribute of the Tweet, expanded_url and display_url
         * for links and media, text for hashtags, and screen_name for user mentions are checked for matches.
         */
        //endpoint.trackTerms(filterTerms);


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

        endpoint.locations(locationsExample);

        //This is to access the tweets.
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);


        BasicClient client = new ClientBuilder()
                .name("TEMClient")
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();

        //Start the connection.
       // client.connect();
        //For now let's set up a counter instead of a timer.
        int total = 10000;
        // Keep the connection open as long as we want to.
        while(keepStreaming) {

            if (client.isDone()) {
                System.out.println("Connections closed! : " + client.getExitEvent().getMessage());
                break;
            }

            //For now let's set up a counter instead of a timer.
            total--;
            if(total == 0){ keepStreaming = false; }
        }
        //Stop the connection.
      //  client.stop();
    }

    public void stopStreaming(){
        keepStreaming = false;
    }

    public void readStream(){
        while(!queue.isEmpty()){
            try {
                String message = queue.take();
                System.out.println(message);
            }catch(Exception e){
                //e.printStackTrace();
            }
        }
    }

    public String getMessage(){
        try {
            return queue.take();
        }catch(Exception e){
            //e.printStackTrace();
            return "FAIL"; //TODO handle this exception gracefully like swan, but fiercely like a lioness.
        }
    }

    public void updateLogin(){
       JSONParser parser = new JSONParser();

        try {

            Object obj = parser.parse(new FileReader(
                    "/home/irvin/Documents/Universidad/VIISemester/BasesII/Proyecto/TwEatingMach/src/main/java/com/tweatingmach/twitterLogin.txt"));

            JSONObject jsonObject = (JSONObject) obj;

            consumerKey = (String) jsonObject.get("consumerKey");
            consumerSecret = (String) jsonObject.get("consumerSecret");
            token = (String) jsonObject.get("token");
            secret = (String) jsonObject.get("secret");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
