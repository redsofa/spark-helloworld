package ca.redsofa.jobs;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Arrays;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.callUDF;

public class HelloWorld {
    private static String INPUT_FILE = "/home/richardr/Documents/data/inputdata.txt";

    public static void registerStringLengthUdf(SparkSession spark){
        spark.udf().register("stringLengthUdf", new UDF1<String, Long>() {
          @Override
          public Long call(String str) { 
              if(str != null && !str.isEmpty()){
                return new Long(str.length());  
              }else{
                return 0L;
              }
            }
        }, DataTypes.LongType);
    }

    public static void main(String[] args) {
        HelloWorld job = new HelloWorld();
        job.startJob();
    }

    private void startJob( ){
        System.out.println("Stating Job...");

        long startTime = System.currentTimeMillis();	    

        //1 - Start the Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Batch Job")
                .config("spark.driver.memory", "2g")
                .enableHiveSupport()
                .getOrCreate();

        //2 - Read in the text file
        Dataset<String> inputDataDs = spark.read().text(INPUT_FILE).as(Encoders.STRING());

        //3 - Create words data set. Take each line in the inputDataDs and create one row 
        // for each word in the text file.
        
        // Source : https://gist.github.com/lucianogiuseppe/063aff936f548fdd0faad6ef004a43e7
        Dataset<String> words = inputDataDs.flatMap(s -> {
                                    return  Arrays.asList(s.toLowerCase().split(" ")).iterator(); 
                                }, Encoders.STRING())
                                .filter(s -> !s.isEmpty()); 

        words.printSchema();


        //4 - Create a temporary table so we can use SQL queries
        words.createOrReplaceTempView("words");

        //5 - Write and execute query 
        String sql = "SELECT " + 
                        "value as word, " + 
                        "COUNT(value) as word_count " + 
                     "FROM " +
                       "words " +  
                     "GROUP BY " + 
                       "value " +
                     "ORDER BY " + 
                        "value " + 
                     "ASC " ;

        Dataset<Row> wordCount = spark.sql(sql);

        //6 - Show contents of the Dataset
        wordCount.show(10);


        //7 - Register user defined function (UDF)
        registerStringLengthUdf(spark);
        wordCount.createOrReplaceTempView("word_counts");

        //8 - Call UDF from a SQL statement. 
        //    Note : This UDF could be called from R, Python, Java and Scala code
        sql = "SELECT *, stringLengthUdf(word) as my_UDF_str_length FROM word_counts";

        Dataset <Row> wordCountWithLengths = spark.sql(sql);
        wordCountWithLengths.show(10);


        //9 - Call UDF (again) but now by using the withColumn method
        Dataset <Row> withColumnDs = wordCount
                                        .withColumn("my_UDF_str_len",
                                          callUDF("stringLengthUdf", wordCount.col("word"))
                                        );

        withColumnDs.show(10);

        //10 - Stop Spark context
        spark.stop();

        //11 - Show execution time info...
        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Execution time in ms : " + elapsedTime);
    }
}