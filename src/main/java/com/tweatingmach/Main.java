package com.tweatingmach;


import com.twitter.hbc.core.endpoint.Location;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;

import java.util.*;

public class Main {
    /**
     * Simple wordcount on tweets arriving every 10 seconds
     * @param args
     */
    public static void main(String[] args) {

        List<String> filters = new ArrayList<String>();
        filters.add("Luis Guillermo Solís");
        filters.add("@luisguillermosr");
        filters.add("Juan Diego Castro Fernández");
        filters.add("@JDiegoCastroCR");
        filters.add("Antonio Álvarez Desanti");
        filters.add("@desanti1234alv");
        filters.add("Rodolfo Piza Rocafort ");
        filters.add("@PizaRodolfo");
        filters.add("Carlos Alvarado");
        filters.add("@CarlosAlvQ");
        filters.add("Edgardo Araya");
        filters.add("@GardodeCQ");
        filters.add("Otto Guevara Guth");
        filters.add("@OttoGuevaraG");
        filters.add("Keylor Navas");
        filters.add("@NavasKeylor");
        //filters.add("Hoy");
        //filters.add("Trump");
        filters.add("Apple");


       /*
        TwitterSource twitterSource = new TwitterSource();

        try {
            twitterSource.streamData(filters, null);
        }catch(Exception e){
            e.printStackTrace();
        }

        try{
            Thread.sleep(10000);
        }
        catch(InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }*/

        SparkConf conf = new SparkConf().setAppName("TwEatingMachine").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); //set batch interval to 10s
        JavaReceiverInputDStream<String> stream = jssc.receiverStream(new JavaTwitterCustomReceiver(filters,null));


        //Parse the text into objects.
        JavaDStream<JSONObject> tweetObjects = stream.map(new Function<String, JSONObject>() {
            @Override
            public JSONObject call(String tweet) {
                JSONObject tweetObject;
                try {
                    tweetObject = (JSONObject)new JSONParser().parse(tweet);
                    //System.out.println("The tweet: "++""+tweetObject.get("text"));
                }catch(Exception e){
                    tweetObject = new JSONObject();
                }
                return tweetObject;
            }
        });


        /*
        	Users can amplify the broadcast of Tweets authored by other users by retweeting .
        	Retweets can be distinguished from typical Tweets by the existence of a
        	retweeted_status attribute. This attribute contains a representation of the original
        	Tweet that was retweeted. Note that retweets of retweets do not show representations
        	of the intermediary retweet, but only the original Tweet. (Users can also unretweet
        	a retweet they created by deleting their retweet.)
        */

        //Take only the non retweets.
        JavaDStream<JSONObject> originalTweets = tweetObjects.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject tweet) {
                if(tweet.containsKey("retweeted_status")){
                    //System.out.println("Retweeted!");
                    return false;
                }else{
                    //System.out.println("I am not retweeted");
                    return true;
                }
            }
        });


        //Take only original in English
        JavaDStream<JSONObject> oriEnglishTweets = originalTweets.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject tweet) {
                if(tweet.get("lang").equals("en")){
                    System.out.println("English detected");
                    return true;
                }else{
                    System.out.println("I am not in english");
                    return false;
                }
            }
        });


        //Take only original in spanish
        JavaDStream<JSONObject> oriSpanishTweets = originalTweets.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject tweet) {
                try {
                    if (tweet.get("lang").equals("es")) {
                        System.out.println("Spanish detected ");
                        return true;
                    } else {
                        //System.out.println("I am not in Spanish");
                        return false;
                    }
                }catch (NullPointerException e){
                    System.out.println("Exception "+e.getMessage());
                    return false;
                }
            }
        });


        //Take only original tweets with geolocation.
        JavaDStream<JSONObject> geoTweets = oriSpanishTweets.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject tweet) {
                if(tweet.get("coordinates")==null){
                    //System.out.println("I have no coordinates: "+tweet.get("coordinates"));
                    return false;
                }else{
                    System.out.println("Coordinates: "+tweet.get("coordinates"));
                    return true;
                }
            }
        });

        //Take only original tweets with geolocation.
        JavaDStream<JSONObject> tweetsWithCity = oriEnglishTweets.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject tweet) {
                if(tweet.get("place")==null){
                    System.out.println("The place is null ");
                    return false;
                }else{
                    JSONObject tweetObject;
                    try {
                        tweetObject = (JSONObject)new JSONParser().parse(tweet.get("place").toString());
                        System.out.println("Place name: "+tweetObject.get("full_name"));
                    }catch(Exception e){
                        System.out.println("Exception "+e.getMessage());
                    }
                    return true;
                }
            }
        });

        JavaPairDStream<String, Integer> tweetsSentiments = oriEnglishTweets.mapToPair( new PairFunction<JSONObject, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(JSONObject tweet) {
                String the_tweet = tweet.get("text").toString();
                if(the_tweet==null){
                    System.out.println("No tweet");
                    return new Tuple2<>("No tweet", 0);
                }else{
                    NLProcessor sentimentProcessor = new NLProcessor();
                    sentimentProcessor.initialize();
                    try{
                        return new Tuple2<>(the_tweet+": ", sentimentProcessor.findSentiment(the_tweet));
                    }catch (Exception e){
                        System.out.println("Error processing sentiment "+e.getMessage());
                        return new Tuple2<>("No tweet", 0);
                    }
                }
            }
        });

/*
        //split stream into words
        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                System.out.println(s);
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //UNUSED filter by hashtags
        JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) {
                return word.startsWith("#");
            }
        });

        //MAP
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<>(s, 1);
                    }
                });
        //REDUCE
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });

        //print to console
        wordCounts.print();*/
        //oriSpanishTweets.print();
        tweetsSentiments.print();
        //start streaming
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

       //twitterSource.readStream();

    }
}



