import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple1;
import scala.Tuple3;
import scala.Tuple4;

import java.sql.Date;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class NyTest {

      //private static List<SupportClass3> supportClasses3;

      public static void main(String[] args) {
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);

            /*TO RUN IN LOCAL WITHOUT SPECIFYING PARAM*/

            String file = "";
            String master = "local[4]";
            if(args.length >= 1) {
                  master = args[0];
                  file = args.length >= 2 ? args[1] : "";
            }

            if(file.equals("")) {
                  System.out.println("ERROR: insert file path and name");
                  return;
            }

            final SparkSession spark = SparkSession
                        .builder()
                        .appName("NyTest")
                        .master(master)
                        .getOrCreate();

            //Load data
            long startLoading = System.nanoTime();
            final List<StructField> schemaFields = new ArrayList<>();
            schemaFields.add(DataTypes.createStructField("date", DataTypes.DateType, true));
            schemaFields.add(DataTypes.createStructField("time", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("borough", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("zip", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("latitude", DataTypes.FloatType, true));
            schemaFields.add(DataTypes.createStructField("longitude", DataTypes.FloatType, true));
            schemaFields.add(DataTypes.createStructField("location", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("on_street_name", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("cross_street_name", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("off_street_name", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("n_person_injured", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_person_killed", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_pedestrian_injured", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_pedestrian_killed", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_cyclist_injured", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_cyclist_killed", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_motor_injured", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("n_motor_killed", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("factor1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("factor2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("factor3", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("factor4", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("factor5", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("key", DataTypes.IntegerType, true));
            schemaFields.add(DataTypes.createStructField("vehicle_type1", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("vehicle_type2", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("vehicle_type3", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("vehicle_type4", DataTypes.StringType, true));
            schemaFields.add(DataTypes.createStructField("vehicle_type5", DataTypes.StringType, true));

            final StructType schema = DataTypes.createStructType(schemaFields);

            //path file will be like s3n://bucket_name//file_name.csv
            final Dataset<Row> dataSet = spark
                        .read()
                        .option("header", "true")
                        .option("dateFormat", "MM/dd/yyyy")
                        .option("delimiter", ",")
                        .schema(schema)
                        .csv(file)
                        .select("date",
                                    "borough",
                                    "n_person_killed",
                                    "n_pedestrian_killed",
                                    "n_cyclist_killed",
                                    "n_motor_killed",
                                    "factor1",
                                    "factor2",
                                    "factor3",
                                    "factor4",
                                    "factor5")
                        ;

            dataSet.persist();

            long loadingTime = System.nanoTime() - startLoading;
            //end of loading

            //query1
            long startQuery1Time = System.nanoTime();

            final Dataset<Row> minMaxDates = dataSet
                        .select("date")
                        .agg(min("date"), max("date"));

            final Date globalMinDate = minMaxDates.first().getDate(0);
            final Date globalMaxDate = minMaxDates.first().getDate(1);

            final Dataset<Row> lethalAccidentsSet = dataSet
                        .withColumn("sum_death",
                                    col("n_person_killed").plus(
                                    col("n_pedestrian_killed").plus(
                                    col("n_cyclist_killed").plus(
                                    col("n_motor_killed")))))
                        .filter(col("sum_death").gt(0))
                        .drop("n_person_killed", "n_pedestrian_killed", "n_cyclist_killed", "n_motor_killed");

            lethalAccidentsSet.persist();

            int daysNo = (int)ChronoUnit.DAYS.between(globalMinDate.toLocalDate(), globalMaxDate.toLocalDate()) + 1;
            int tempWeekNo = daysNo / 7;
            if((daysNo % 7) != 0) {
                  tempWeekNo ++;
            }
            final int weeksNo = tempWeekNo;


            int [] lethalAccidents = new int[weeksNo];
            int totalLethalAccidents = 0;

            JavaRDD<Tuple1<Integer>> indexesMap = lethalAccidentsSet.toJavaRDD().map(row -> {
                  int index = (int)(ChronoUnit.DAYS.between
                              (globalMinDate.toLocalDate(), row.getDate(0).toLocalDate()))+ 1;
                  if(index != 0) {
                        if(index % 7 == 0) {
                              index = (index/7) -1;
                        }
                        else {
                              index = index/7;
                        }
                  }
                  return new Tuple1<>(index);
            });

            //dont know if slower or faster
            //Map<Tuple1<Integer>, Long> indexesCounted = indexesMap.countByValue();
            //indexesCounted.forEach((t, num) -> lethalAccidents[t._1()] = num.intValue());
            //wrt to:
            indexesMap.collect().forEach(t -> lethalAccidents[t._1()] ++);
            //in any case (not done in a parallel way)

            System.out.print("\nQUERY 1:\n");
            for(int i = 0; i < weeksNo; i++) {
                  System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethalAccidents[i], "lethal accidents");

                  totalLethalAccidents += lethalAccidents[i];
            }
            System.out.printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo, (100.0f*totalLethalAccidents)/weeksNo);

            long query1Time = System.nanoTime() - startQuery1Time;
            //end of query1

            //query2
            long startQuery2Time = System.nanoTime();

            final Dataset<Row> accidentsCountPerFacSet = dataSet
                        .groupBy(
                                    "factor1",
                                    "factor2",
                                    "factor3",
                                    "factor4",
                                    "factor5")
                        .count()
                        ;

            final Dataset<Row> deathsPerFacSet = accidentsCountPerFacSet
                        .join(lethalAccidentsSet,
                                    lethalAccidentsSet.col("factor1").eqNullSafe(accidentsCountPerFacSet.col("factor1"))
                                                .and(lethalAccidentsSet.col("factor2").eqNullSafe(accidentsCountPerFacSet.col("factor2")))
                                                .and(lethalAccidentsSet.col("factor3").eqNullSafe(accidentsCountPerFacSet.col("factor3")))
                                                .and(lethalAccidentsSet.col("factor4").eqNullSafe(accidentsCountPerFacSet.col("factor4")))
                                                .and(lethalAccidentsSet.col("factor5").eqNullSafe(accidentsCountPerFacSet.col("factor5"))))
                        .select(lethalAccidentsSet.col("factor1"),
                                    lethalAccidentsSet.col("factor2"),
                                    lethalAccidentsSet.col("factor3"),
                                    lethalAccidentsSet.col("factor4"),
                                    lethalAccidentsSet.col("factor5"),
                                    lethalAccidentsSet.col("sum_death"))
                        .groupBy(
                                    "factor1",
                                    "factor2",
                                    "factor3",
                                    "factor4",
                                    "factor5")
                        .sum("sum_death")
                        ;

            //THIS JOIN MUST BE LEFT OUTER!!!
            //IN ORDER TO MANTAIN ALSO THAT TUPLES FOR WHICH NUMBER OF DEATHS IS 0
            //AND SELECTION MUST BE THAT FOR THE SAME REASON
            final Dataset<Row> joined = accidentsCountPerFacSet
                        .join(deathsPerFacSet,
                                    deathsPerFacSet.col("factor1").eqNullSafe(accidentsCountPerFacSet.col("factor1"))
                                                .and(deathsPerFacSet.col("factor2").eqNullSafe(accidentsCountPerFacSet.col("factor2")))
                                                .and(deathsPerFacSet.col("factor3").eqNullSafe(accidentsCountPerFacSet.col("factor3")))
                                                .and(deathsPerFacSet.col("factor4").eqNullSafe(accidentsCountPerFacSet.col("factor4")))
                                                .and(deathsPerFacSet.col("factor5").eqNullSafe(accidentsCountPerFacSet.col("factor5")))
                                    , "full_outer")
                        .select(accidentsCountPerFacSet.col("factor1"),
                                    accidentsCountPerFacSet.col("factor2"),
                                    accidentsCountPerFacSet.col("factor3"),
                                    accidentsCountPerFacSet.col("factor4"),
                                    accidentsCountPerFacSet.col("factor5"),
                                    accidentsCountPerFacSet.col("count"),
                                    deathsPerFacSet.col("sum(sum_death)"))
                        ;
            //table with factor1 ... factor5 count sum(sum_death)

            Map<String, SupportClass2> supportClasses2 = Collections.synchronizedMap(new HashMap<>());

            JavaRDD<Tuple3<List<String>, Integer, Integer>> resultsPerFactor = joined.toJavaRDD().map(row -> {
                  List<String> toAdd = new ArrayList<>();
                  for(int i = 0; i < row.size() - 2; i++) {
                        if(row.getString(i) != null && !toAdd.contains(row.getString(i))) {
                              toAdd.add(row.getString(i));
                        }
                  }
                  int lethal = row.get(6) != null ? (int)row.getLong(6) : 0;
                  return new Tuple3<>(toAdd, (int)row.getLong(5), lethal);
            });

            //(not done in a parallel way)
            resultsPerFactor.collect().forEach(t -> {
                  for(String s : t._1()) {
                        //DO NOT INVERT ORDER OF PRESENT/ABSENT (obviously..)
                        supportClasses2.computeIfPresent(s, (sf, vf) -> {
                              vf.addToAccidents(t._2());
                              vf.addToLethals(t._3());
                              return vf;
                        });

                        supportClasses2.computeIfAbsent(s, v -> new SupportClass2(t._2(), t._3()));
                  }
            });

            //int facLen = supportClasses2.size();
            System.out.print("\nQUERY 2:\n");
            System.out.printf("\t%-60s%-20s%-20s%s%n", "FACTOR", "N_ACCIDENTS", "N_DEATHS", "PERC_N_DEATHS");

            for (String s : supportClasses2.keySet()) {
                  SupportClass2 elem = supportClasses2.get(s);
                  double perc = elem.getAccidents() != 0 ?
                              ((elem.getLethals() * 100.0f) / elem.getAccidents()) :
                              0;

                  System.out.printf("\t%-60s%-20d%-20d%.2f%%%n", s, elem.getAccidents(), elem.getLethals(), perc);
            }

            long query2Time = System.nanoTime() - startQuery2Time;
            //end of query2


            //query3
            long startQuery3Time = System.nanoTime();

            final Dataset<Row> boroughSet = dataSet
                        .select("date", "borough")
                        .filter(col("borough").isNotNull())
                        .groupBy("borough", "date")
                        .count()
                        .withColumn("count_accidents", col("count"))
                        .drop("count")
                        .orderBy("borough", "date")
                        ;
            boroughSet.persist();

            final Dataset<Row> deathBorough = lethalAccidentsSet
                        .select("date", "borough")
                        .filter(col("borough").isNotNull())
                        .groupBy("borough", "date")
                        .count()
                        .withColumn("count_deaths", col("count"))
                        .drop("count")
                        .orderBy("borough", "date")
                        ;

            final Dataset<Row> accidentsAndDeathPerBorough = boroughSet
                        .join(deathBorough,
                                    deathBorough.col("borough").eqNullSafe(boroughSet.col("borough"))
                                                .and(deathBorough.col("date").eqNullSafe(boroughSet.col("date")))
                                    ,"full_outer")
                        .select(boroughSet.col("borough"),
                                    boroughSet.col("date"),
                                    boroughSet.col("count_accidents"),
                                    deathBorough.col("count_deaths"))
                        .orderBy("borough", "date")
                        ;

            final Dataset<Row> boroughsNameSet = boroughSet
                        .groupBy("borough")
                        .agg(count("borough"))
                        .orderBy("borough")
                        ;

            List<SupportClass3> supportClasses3 = Collections.synchronizedList(new ArrayList<>());

            //init, useless to JavaRDD ?
            //(not done in a parallel way)
            boroughsNameSet.toJavaRDD().collect().forEach(row -> {
                  supportClasses3.add(
                              new SupportClass3(
                                          row.getString(0),
                                          new int[weeksNo],
                                          0));
            });

            JavaRDD<Tuple4<String, Integer, Integer, Integer>> perBorough =
                        accidentsAndDeathPerBorough.toJavaRDD().map(row -> {
                              int index = (int)(ChronoUnit.DAYS.between(
                                          globalMinDate.toLocalDate(), row.getDate(1).toLocalDate())) + 1;
                              if(index != 0) {
                                    if(index % 7 == 0) {
                                          index = (index/7) -1;
                                    }
                                    else {
                                          index = index/7;
                                    }
                              }

                              int lethal = row.get(3) != null ? (int)row.getLong(3) : 0;

                              return new Tuple4<>(row.getString(0), index, (int)row.getLong(2), lethal);
                        });

            //actually update (not done in a parallel way)
            perBorough.collect().forEach(t -> {
                  for(SupportClass3 s : supportClasses3) {
                        if(s.getBorough().equals(t._1())) {
                              s.getAccidentsPerBoroughPerWeek()[t._2()] += t._3();
                              int prev = s.getLethalsPerBoroughPerWeek();
                              s.setLethalsPerBoroughPerWeek(prev + t._4());
                              break;
                        }
                  }
            });

            System.out.print("\nQUERY 3:\n");
            for (SupportClass3 s : supportClasses3) {
                  System.out.printf("\tBOROUGH: %s\n", s.getBorough());

                  for (int j = 0; j < s.getAccidentsPerBoroughPerWeek().length; j++) {
                        System.out.printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, s.getAccidentsPerBoroughPerWeek()[j], "accidents");
                  }

                  System.out.printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                              (100.0f * s.getLethalsPerBoroughPerWeek()) / s.getAccidentsPerBoroughPerWeek().length,
                              s.getLethalsPerBoroughPerWeek(),
                              s.getAccidentsPerBoroughPerWeek().length);
            }

            long query3Time = System.nanoTime() - startQuery3Time;

            //end of query3

            System.out.println("\nIt took " + loadingTime/1000000000.0f + " seconds to load data (and order it)");
            System.out.println("It took " + query1Time/1000000000.0f + " seconds to calculate query 1");
            System.out.println("It took " + query2Time/1000000000.0f + " seconds to calculate query 2");
            System.out.println("It took " + query3Time/1000000000.0f + " seconds to calculate query 3");

            /*Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            spark.close();*/
      }
}
