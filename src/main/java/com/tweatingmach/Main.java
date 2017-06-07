package com.tweatingmach;


import java.util.ArrayList;
import java.util.List;

public class Main {

    //Just to avoid these:

    //We are adding this


    public static void main(String[] args) {
	// write your code here
        org.apache.log4j.PropertyConfigurator.configure("./src/main/resources/log4j.properties");
        TwitterSource twitt = new TwitterSource();
        List<String> terms = new ArrayList<String>();
        terms.add("Trump");
        System.out.println(System.getProperty("java.class.path") );
        try {
           twitt.Feed(terms,null);
        }catch( Exception e){
            e.printStackTrace();
        }

        twitt.readStream();
    }
}
