package com.tweatingmach;


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
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import scala.Tuple2;
import twitter4j.*;

import java.io.FileReader;
import java.util.Arrays;
import java.util.Iterator;

public class Main {

    /**
     * Simple wordcount on tweets arriving every 10 seconds
     * @param args
     */
    public static void main(String[] args) {


        /*
        JSONParser parser = new JSONParser();
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(new FileReader("./src/main/java/com/tweatingmach/twitterLogin.txt"));
            System.setProperty("twitter4j.oauth.consumerKey", (String) jsonObject.get("consumerKey"));
            System.setProperty("twitter4j.oauth.consumerSecret", (String) jsonObject.get("consumerSecret"));
            System.setProperty("twitter4j.oauth.accessToken", (String) jsonObject.get("token"));
            System.setProperty("twitter4j.oauth.accessTokenSecret", (String) jsonObject.get("secret"));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        SparkConf conf = new SparkConf().setAppName("TwEatingMachine").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); //set batch interval to 10s

        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jssc);

        //split stream into words
        JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
            @Override
            public Iterator<String> call(Status s) {
                return Arrays.asList(s.getText().split(" ")).iterator();
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
        wordCounts.print();

        //start streaming
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        */

        


        try {
            twitt.Feed(terms,null);
        }catch( Exception e){
            e.printStackTrace();
        }

    }
}
