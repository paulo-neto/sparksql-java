package com.pauloneto.apachespark.producer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import javax.annotation.PreDestroy;
import javax.enterprise.inject.Produces;

public class CDIProducer {

    private JavaSparkContext sparkContext;
    private JavaSparkContext sparkContextMongo;

    @Produces @SparkContextMode(type = SparkContextType.DEFAULT)
    public JavaSparkContext createJavaSparkContext(){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("ExemplosApacheSpark");
        sparkContext = new JavaSparkContext(conf);
        return sparkContext;
    }

    @Produces @SparkContextMode(type = SparkContextType.MONGODB)
    public JavaSparkContext createJavaSparkContextMongo(){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExemplosApacheSparkMongo")
                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/local")
                .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/local")
                .getOrCreate();
        sparkContextMongo = new JavaSparkContext(spark.sparkContext());
        return sparkContextMongo;
    }

    @PreDestroy
    public void closeJavaSparkContext(){
        if(sparkContext != null)
            sparkContext.close();

        if(sparkContextMongo != null)
            sparkContextMongo.close();
    }
}
