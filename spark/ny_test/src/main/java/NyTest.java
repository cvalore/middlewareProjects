import org.apache.hadoop.util.Time;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Date;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class NyTest {

      public static void main(String[] args) {

            //<editor-fold desc="SPARK SESSION CREATION">
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);

            /* TO RUN IN LOCAL */
/*
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
*/
            /* TO RUN IN CLOUD SPECIFYING PARAM*/

            String file = "";
            String core_exec = "";
            String mem_exec = "";
            if(args.length >= 1) {
                  file = args[0];
                  core_exec = args.length >= 2 ? args[1] : "";
                  mem_exec = args.length >= 3 ? args[2] : "";
            }

            if(file.equals("") || core_exec.equals("") || mem_exec.equals("")) {
                  System.out.println("ERROR: insert file path and name, then num of exec, then core for exec, then mem for exec");
                  return;
            }



            final SparkSession spark = SparkSession
                        .builder()
                        .appName("NyTest")
                        .config("spark.executor.cores", core_exec)
                        .config("spark.executor.memory", mem_exec)
                        .getOrCreate();



            //</editor-fold>

            //<editor-fold desc="LOAD DATA">
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
                        .withColumn("sum_death",
                              col("n_person_killed").plus(
                              col("n_pedestrian_killed").plus(
                              col("n_cyclist_killed").plus(
                              col("n_motor_killed")))))
                        ;

            dataSet.persist();

            long loadingTime = System.nanoTime() - startLoading;
            //</editor-fold">

            //<editor-fold desc="QUERY 1">
            long startQuery1Time = System.nanoTime();


            final Dataset<Row> minMaxDates = dataSet
                    .select("date")
                    .agg(min("date"), max("date"));

            //trascurabile ma guardando ui si vede che viene skippato lo stage del job
            minMaxDates.persist();

            final Date globalMinDate = minMaxDates.first().getDate(0);
            final Date globalMaxDate = minMaxDates.first().getDate(1);

            final Dataset<Row> lethalAccidentsSet = dataSet.filter(col("sum_death").gt(0))
                        .drop("n_person_killed", "n_pedestrian_killed", "n_cyclist_killed", "n_motor_killed")
                        ;

            lethalAccidentsSet.persist();


            int daysNo = (int)ChronoUnit.DAYS.between(globalMinDate.toLocalDate(), globalMaxDate.toLocalDate()) + 1;
            int weeksNum = daysNo / 7;
            if((daysNo % 7) != 0) {
                  weeksNum ++;
            }

            final int weeksNo = weeksNum;

            Function<Row, Integer> lethAccMap =
                        (Function<Row, Integer>) row -> {
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
                              return index;
                        };

            Map<Integer, Long> lethalAccidentsByIndex = lethalAccidentsSet.toJavaRDD().map(lethAccMap).countByValue();

            long lethAcc;
            int totalLethalAccidents = 0;
            System.out.print("\nQUERY 1:\n");
            for(int i = 0; i < weeksNo; i++) {
                  if (lethalAccidentsByIndex.get(i) != null)
                        lethAcc = lethalAccidentsByIndex.get(i);
                  else
                        lethAcc = 0;
                  System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethAcc, "lethal accidents");            //problema se lookup di i non c'è?

                  totalLethalAccidents += lethAcc;
            }

            System.out.printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo, (100.0f*totalLethalAccidents)/weeksNo);

/*
            int [] lethAccidents = new int[weeksNo];
            lethalAccidentsByIndex.forEach((k, v) -> lethAccidents[k] += v);
            int totalLethalAccidents = 0;

            System.out.print("\nQUERY 1:\n");
            for(int i = 0; i < weeksNo; i++) {
                  System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethAccidents[i], "lethal accidents");            //problema se lookup di i non c'è?

                  totalLethalAccidents += lethAccidents[i];
            }


            System.out.printf("\t%d total lethal accidents over %d weeks, avg = %.2f%%\n", totalLethalAccidents, weeksNo, (100.0f*totalLethalAccidents)/weeksNo);
*/

            long query1Time = System.nanoTime() - startQuery1Time;
            //</editor-fold>

            //<editor-fold desc="QUERY 2">
            long startQuery2Time = System.nanoTime();


            PairFlatMapFunction<Row, String, SupportClass2> FromContribFactToRDD =
                        (PairFlatMapFunction<Row, String, SupportClass2>) row -> {
                              List<Tuple2<String, SupportClass2>> rowContribFact = new ArrayList<>();
                              List<String> rowContribFactNames = new ArrayList<>();
                              int lethal = row.getAs("sum_death");
                              //6 = factor1 -> 10 factor5
                              for(int i = 6; i < 11; i++) {
                                    String s = (String)row.get(i);
                                    if (s != null && !rowContribFactNames.contains(s)) {
                                          Tuple2<String, SupportClass2> contribF = new Tuple2<>((String) row.get(i), new SupportClass2(1, lethal));
                                          rowContribFact.add(contribF);
                                          rowContribFactNames.add(s);
                                    }
                              }
                              return rowContribFact.iterator();
                        };

            JavaPairRDD<String, SupportClass2> contribFacts = dataSet.toJavaRDD().flatMapToPair(FromContribFactToRDD);

            Function<SupportClass2, SupportClass2> createAcc = (Function<SupportClass2, SupportClass2>) x -> x;

            Function2<SupportClass2, SupportClass2, SupportClass2> addLocal =
                        (Function2<SupportClass2, SupportClass2, SupportClass2>) (partial, x) -> new SupportClass2(partial.getAccidents() + 1, partial.getLethals() + x.getLethals());

            Function2<SupportClass2, SupportClass2, SupportClass2> combine =
                        (Function2<SupportClass2, SupportClass2, SupportClass2>) (localA, localB) -> new SupportClass2(localA.getAccidents() + localB.getAccidents(),
                                localA.getLethals() + localB.getLethals());

            JavaPairRDD<String, SupportClass2> combinedContribFacts = contribFacts.combineByKey(createAcc, addLocal, combine);

            System.out.print("\nQUERY 2:\n");
            System.out.printf("\t%-60s%-20s%-20s%s%n", "FACTOR", "N_ACCIDENTS", "N_DEATHS", "PERC_N_DEATHS");

            //(not done in parallel - although in this is case is possible)
            combinedContribFacts.collect().forEach(elem -> {
                  final double perc = elem._2().getAccidents() != 0 ?
                          ((elem._2().getLethals()*100.0f)/elem._2().getAccidents()) :
                          0;

                  System.out.printf("\t%-60s%-20d%-20d%.2f%%%n", elem._1(), elem._2().getAccidents(), elem._2().getLethals(), perc);
            });

            long query2Time = System.nanoTime() - startQuery2Time;
            //</editor-fold>

            //<editor-fold desc="QUERY 3">
            long startQuery3Time = System.nanoTime();

            PairFunction<Row, String, SupportClass3a> toSupportClass3a =
                        (PairFunction<Row, String, SupportClass3a>) row -> {

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
                              int lethal = (int)row.getAs("sum_death") > 0 ? 1 : 0;
                              return new Tuple2<>(row.getString(1), new SupportClass3a(index, lethal));
            };


            Function<SupportClass3a, SupportClass3b> createComb =
                        (Function<SupportClass3a, SupportClass3b>) x -> {

                              SupportClass3b supportClass3b = new SupportClass3b(weeksNo, 0);
                              supportClass3b.addAccident(x.getIndex());
                              supportClass3b.addLethalAcc(x.getLethal());
                              return supportClass3b;
            };

            Function2<SupportClass3b, SupportClass3a, SupportClass3b> mergeValue =
                        (Function2<SupportClass3b, SupportClass3a, SupportClass3b>) (partial, x) -> {

                              partial.addAccident(x.getIndex());
                              partial.addLethalAcc(x.getLethal());
                              return partial;
                        };

            Function2<SupportClass3b, SupportClass3b, SupportClass3b> mergeCombine =
                        (Function2<SupportClass3b, SupportClass3b, SupportClass3b>) (localA, localB) -> {

                              localA.addLethalAcc(localB.getLethalsAcc());
                              localA.addLethalAccidents(localB.getAccidentsPerBoroughPerWeek());
                              return localA;
                        };

            JavaPairRDD<String, SupportClass3a> boroughAccidents = dataSet.toJavaRDD()
                        .filter(row -> row.getString(1) != null)
                        .mapToPair(toSupportClass3a);

            JavaPairRDD<String, SupportClass3b> boroughAccidentsPerWeek = boroughAccidents
                        .combineByKey(createComb, mergeValue, mergeCombine);

            System.out.print("\nQUERY 3:\n");

            boroughAccidentsPerWeek.collect().forEach(borough -> {
                  System.out.printf("\tBOROUGH: %s\n", borough._1());

                  for(int j = 0; j < weeksNo; j++) {
                        System.out.printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, borough._2().getAccidentsPerBoroughPerWeek()[j] , "accidents");
                  }

                  System.out.printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                          (100.0f * borough._2().getLethalsAcc()/weeksNo),
                          borough._2().getLethalsAcc(), weeksNo);
            });

            long query3Time = System.nanoTime() - startQuery3Time;
            //</editor-fold>

            //<editor-fold desc="PRINTING">
            System.out.println("\nIt took " + loadingTime/1000000000.0f + " seconds to load data (and order it)");
            System.out.println("It took " + query1Time/1000000000.0f + " seconds to calculate query 1");
            System.out.println("It took " + query2Time/1000000000.0f + " seconds to calculate query 2");
            System.out.println("It took " + query3Time/1000000000.0f + " seconds to calculate query 3");

            /*Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            spark.close();*/

            //</editor-fold>

      }
}
