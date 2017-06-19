package com.tweatingmach;

import com.twitter.hbc.core.endpoint.Location;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import java.util.List;

/**
 * Created by irvin on 6/7/17.
 */
public class JavaTwitterCustomReceiver extends Receiver<String> {

    private  TwitterSource twitterSource;
    List<String> filterTerms;
    List<Location> locations;

    public JavaTwitterCustomReceiver(List<String> filterTerms, List<Location> locations) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        this.filterTerms = filterTerms;
        twitterSource = new TwitterSource();
        this.locations = locations;
    }

    public void onStart() {
        // Start the thread that receives data over a connection
        new Thread()  {
            @Override public void run() {
                receive();
            }
        }.start();
    }

    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself if isStopped() returns false
        System.out.println("I am DONE");
    }

    /** Create Twitter connection and receive data until receiver is stopped */
    private void receive() {

        try {
            // connect to the server
            System.out.println("CONNECTING WITH TWITTER!");
            try {
                twitterSource.streamData(filterTerms, null);
            }catch(Exception e){
                e.printStackTrace();
            }
            //FEEDING
            while (!twitterSource.isDoneBasic())
            {
                String tweet = twitterSource.getTweet();

                if(!tweet.equals("NO-TWEET")) {
                    safePrintln("Received data '" + tweet + "'");
                    store(tweet);
                }
            }
            twitterSource.stopStreaming(); //Feels like this isn't needed.

            // Restart in an attempt to connect again when server is active again
            restart("Trying to connect again");
        } catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }


    public void updateTerms(List<String> filterTerms){
        this.filterTerms = filterTerms;
    }

    public void updateLocations(List<Location> locations){
        this.locations = locations;
    }

    public void safePrintln(String s) {
        synchronized (System.out) {
            System.out.println(s);
        }
    }

}
