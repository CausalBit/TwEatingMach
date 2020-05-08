
package com.tweatingmach;


import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.receiver.Receiver;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;

import java.util.*;

public class Main {
    /**
     * Simple wordcount on tweets arriving every 10 seconds
     *
     * @param args
     */
    public static void main(String[] args) {

        List<String> filters = new ArrayList<String>();
        filters.add("game of thrones");
        filters.add("Game of Thrones");
        filters.add("#GoT");
        filters.add("#winterishere");
        filters.add("#winteriscoming");
        filters.add("#gameofthrones");

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
        /*JavaReceiverInputDStream<String> stream = jssc.receiverStream(new JavaTwitterCustomReceiver(filters,null));*/

        /*JavaRDD<String> textos = jssc.sparkContext().parallelize(Arrays.asList("hola", "como"));

        PriorityQueue<JavaRDD<String>> queue = new PriorityQueue<>();
        queue.add(textos);
        JavaDStream<String> stream = jssc.queueStream(queue);
*/
        JavaReceiverInputDStream<String> stream = jssc.receiverStream(new Receiver<String>(StorageLevel.MEMORY_ONLY()) {


            @Override
            public void onStop() {

            }

            @Override
            public void onStart() {
                Random random = new Random();
                String[] words = new String[]{"{\"text\": \"The worst thing is you\"}", "{\"text\": \"I love you\"}", "{\"text\": \"es lo mejor de la vida\"}"};
                for (int i = 0; i < 100; i++){
                    Integer salt = random.nextInt(2);
                    store(words[(i + salt) % words.length]);
                }
            }
        });

        ArrayList<Accumulator<Integer>> acc = new ArrayList<>();
        for (int i = 0; i < 5; i++)
            acc.add(jssc.sparkContext().accumulator(0));


        //Parse the text into objects.
        JavaDStream<JSONObject> tweetObjects = stream.map(tweet -> {
            JSONObject tweetObject;
            try {
                tweetObject = (JSONObject) new JSONParser().parse(tweet);
                //System.out.println("The tweet: "++""+tweetObject.get("text"));
            } catch (Exception e) {
                tweetObject = new JSONObject();
            }
            return tweetObject;
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
        JavaDStream<JSONObject> originalTweets = tweetObjects.filter(tweet -> {
            if (tweet.containsKey("retweeted_status")) {
                //System.out.println("Retweeted!");
                return false;
            } else {
                //System.out.println("I am not retweeted");
                return true;
            }
        });
                //Take only original in English
                /*.filter(tweet -> {
                    if (tweet.get("lang") == null) {
                        return false;
                    } else if (tweet.get("lang").equals("en")) {
                        System.out.println("English detected");
                        return true;
                    } else {
                        System.out.println("I am not in english");
                        return false;
                    }
                })*/
                //Take only original tweets with geolocation.
                /*.filter(tweet -> {
                    if (tweet.get("place") == null) {
                        System.out.println("The place is null ");
                        return false;
                    } else {
                        JSONObject tweetObject;
                        try {
                            tweetObject = (JSONObject) new JSONParser().parse(tweet.get("place").toString());
                            System.out.println("Place name: " + tweetObject.get("full_name"));
                        } catch (Exception e) {
                            System.out.println("Exception " + e.getMessage());
                        }
                        return true;
                    }
                });*/


        //Take only original in spanish
        JavaDStream<JSONObject> oriSpanishTweets = originalTweets.filter(tweet -> {
            try {
                if (tweet.get("lang") == null) {
                    return false;
                } else if (tweet.get("lang").equals("es")) {
                    System.out.println("Spanish detected ");
                    return true;
                } else {
                    //System.out.println("I am not in Spanish");
                    return false;
                }
            } catch (NullPointerException e) {
                System.out.println("Exception " + e.getMessage());
                return false;
            }
        })


                //Take only original tweets with geolocation.
                .filter(tweet -> {
                    if (tweet.get("coordinates") == null) {
                        //System.out.println("I have no coordinates: "+tweet.get("coordinates"));
                        return false;
                    } else {
                        System.out.println("Coordinates: " + tweet.get("coordinates"));
                        return true;
                    }
                });


        JavaPairDStream<Integer, Integer> tweetsSentiments = originalTweets.mapToPair(tweet -> {
            String the_tweet = tweet.get("text").toString();
            if (the_tweet == null) {
                System.out.println("No tweet");
                return new Tuple2<>("No tweet", 0);
            } else {
                NLProcessor sentimentProcessor = new NLProcessor();
                NLProcessor.initialize();
                try {
                    return new Tuple2<>(the_tweet + ": ", NLProcessor.findSentiment(the_tweet));
                } catch (Exception e) {
                    System.out.println("Error processing sentiment " + e.getMessage());
                    return new Tuple2<>("No tweet", 0);
                }
            }
        })

        //MAP for sentiment frequency count
        .mapToPair(
                (PairFunction<Tuple2<String, Integer>, Integer, Integer>) t -> new Tuple2<>(t._2, 1))

        //REDUCE for sentiment frequency count
        .reduceByKey(
                (Function2<Integer, Integer, Integer>) Integer::sum);

        //Accumulate
        tweetsSentiments.foreachRDD(
                (VoidFunction<JavaPairRDD<Integer, Integer>>) pair -> {
                    Map<Integer, Integer> map = pair.collectAsMap();
                    for (int i = 0; i < 5; i++) {
                        if (map.containsKey(i))
                            acc.get(i).add(map.get(i));
                        System.out.println("Acc " + i + ": " + acc.get(i).value().toString());
                    }
                }
        );

        tweetsSentiments.print();

        //start streaming
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}


