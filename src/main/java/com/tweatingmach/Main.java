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
import scala.Tuple2;

import java.util.*;

public class Main {
    /**
     * Simple wordcount on tweets arriving every 10 seconds
     * @param args
     */
    public static void main(String[] args) {

        List<String> filters = new ArrayList<String>();
        filters.add("Trump");
        //We can also add locations from here and pass them as parameters.

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

        SparkConf conf = new SparkConf().setAppName("TwEatingMachine").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10)); //set batch interval to 10s
        JavaReceiverInputDStream<String> stream = jssc.receiverStream(new JavaTwitterCustomReceiver(filters,null));

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
        wordCounts.print();

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



