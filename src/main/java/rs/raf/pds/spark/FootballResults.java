package rs.raf.pds.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class FootballResults {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("FootballResults")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("OFF");

        JavaRDD<String> lines = spark.sparkContext()
                .textFile("dataset/results.csv", 0)
                .toJavaRDD();

        String header = lines.first();

        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        JavaPairRDD<String, Integer> goals = data.flatMapToPair(line -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();

            try {
                String[] p = line.split(",");

                if (p.length >= 5) {
                    String homeTeam = p[1].trim();
                    String awayTeam = p[2].trim();
                    int homeScore = Integer.parseInt(p[3].trim());
                    int awayScore = Integer.parseInt(p[4].trim());

                    list.add(new Tuple2<>(homeTeam, homeScore));
                    list.add(new Tuple2<>(awayTeam, awayScore));
                }

            } catch (Exception e) {
                // Preskakanje neispravnih redova
            }

            return list.iterator();
        });

        JavaPairRDD<String, Integer> totalGoals = goals.reduceByKey(Integer::sum);

        JavaPairRDD<Integer, String> sortedGoals = totalGoals
                .mapToPair(Tuple2::swap)
                .sortByKey(false);

        System.out.println("\nZADATAK 1.1 - Najefikasnije reprezentacije");
        System.out.println("Team, TotalGoals");

        sortedGoals.take(10).forEach(row ->
                System.out.println(row._2 + ", " + row._1)
        );

        Dataset<Row> resultsDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("dataset/results.csv");

        Dataset<Row> worldCupStats = resultsDF
                .filter(col("tournament").equalTo("FIFA World Cup"))
                .withColumn("Year", year(to_date(col("date"))))
                .withColumn("MatchGoals", col("home_score").plus(col("away_score")))
                .groupBy("Year")
                .agg(
                        count("*").as("Matches"),
                        sum("MatchGoals").as("TotalGoals"),
                        avg("MatchGoals").as("AvgGoalsPerMatch")
                )
                .orderBy(col("AvgGoalsPerMatch").desc());

        System.out.println("\nZADATAK 1.2 - FIFA World Cup statistika po godinama");
        worldCupStats.show(100, false);

        spark.stop();
    }
}