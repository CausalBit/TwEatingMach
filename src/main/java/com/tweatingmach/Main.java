
package com.tweatingmach;



import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;


import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;
import scala.Tuple2;

import java.util.*;

public class Main {
    /**
     * Simple wordcount on tweets arriving every 10 seconds
     *
     * @param args
     */
    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("TwEatingMachine").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1)); //set batch interval to 10s

        String topics = "kafka-chat";
        String brokers = "localhost:9092";
        String groupId = "kafka-sandbox";
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.CLIENT_ID_CONFIG, "main");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, kafkaParams));

        ArrayList<Accumulator<Integer>> acc = new ArrayList<>();
        for (int i = 0; i < 5; i++)
            acc.add(jssc.sparkContext().accumulator(0));


        //Parse the text into objects.
        JavaDStream<JSONObject> tweetObjects = stream.map(tweet -> {
            JSONObject tweetObject = new JSONObject();
            tweetObject.<String,String>put("text", tweet.value());
            return tweetObject;
        });

        JavaPairDStream<Integer, Integer> tweetsSentiments = tweetObjects.mapToPair(tweet -> {
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


