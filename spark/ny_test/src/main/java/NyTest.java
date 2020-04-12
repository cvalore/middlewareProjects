import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.spark.sql.functions.*;

public class NyTest {

      private static AtomicReferenceArray<Integer> lethalAccidents;
      private static List<SupportClass3> supportClasses3;

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
                        .orderBy("date");

            dataSet.persist();

            long loadingTime = System.nanoTime() - startLoading;
            //end of loading

            //query1
            long startQuery1Time = System.nanoTime();

            final Date globalMinDate = dataSet.first().getDate(0);
            final Date globalMaxDate = dataSet
                        .select("date")
                        .agg(max("date"))
                        .first()
                        .getDate(0);

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
            if((daysNo != 0) && ((daysNo % 7) != 0)) {
                  tempWeekNo ++;
            }
            final int weeksNo = tempWeekNo;

            lethalAccidents = new AtomicReferenceArray<>(weeksNo);
            for(int i = 0; i < weeksNo; i++) {
                  lethalAccidents.set(i, 0);
            }
            int totalLethalAccidents = 0;

            lethalAccidentsSet.foreach((ForeachFunction<Row>) row -> {
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
                  lethalAccidents.getAndAccumulate(index, 1, Integer::sum);
            });

            System.out.print("\nQUERY 1:\n");
            for(int i = 0; i < weeksNo; i++) {
                  System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethalAccidents.get(i), "lethal accidents");

                  totalLethalAccidents += lethalAccidents.get(i);
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
            //table with count factor1 ... factor5 sum(sum_death)

            List<Row> groupedRows = joined.toJavaRDD().collect();

            List<SupportClass2> supportClasses2 = Collections.synchronizedList(new ArrayList<>());

            //int len = groupedRows.size();
            int rowSize = groupedRows.size() > 0 ? groupedRows.get(0).size() : 0;

            for (Row r : groupedRows) {
                  List<SupportClass2> partialSupport = Collections.synchronizedList(new ArrayList<>());

                  for (int j = 0; j < rowSize - 2; j++) {
                        if (r.getString(j) != null && findInListOfSupport(partialSupport, r.getString(j)) == null) {
                              SupportClass2 temp = new SupportClass2(
                                          r.getString(j), r.getLong(rowSize - 2), 0L);
                              if (r.get(rowSize - 1) != null)
                                    temp.setLethals(r.getLong(rowSize - 1));
                              partialSupport.add(temp);
                        }
                  }
                  //int size = partialSupport.size();
                  for (SupportClass2 s2 : partialSupport) {
                        SupportClass2 supportFound = findInListOfSupport(supportClasses2, s2.getFactor());
                        if (supportFound == null) {
                              SupportClass2 temp = new SupportClass2(s2.getFactor(), s2.getAccidents(), s2.getLethals());
                              supportClasses2.add(temp);
                        } else {
                              supportFound.setAccidents(supportFound.getAccidents() + s2.getAccidents());
                              supportFound.setLethals(supportFound.getLethals() + s2.getLethals());
                        }
                  }
            }

            //int facLen = supportClasses2.size();
            System.out.print("\nQUERY 2:\n");
            System.out.printf("\t%-60s%-20s%-20s%s%n", "FACTOR", "N_ACCIDENTS", "N_DEATHS", "PERC_N_DEATHS");

            for (SupportClass2 s : supportClasses2) {
                  double perc = s.getAccidents() != 0 ?
                              ((s.getLethals() * 100.0f) / s.getAccidents()) :
                              0;

                  System.out.printf("\t%-60s%-20d%-20d%.2f%%%n", s.getFactor(), s.getAccidents(), s.getLethals(), perc);
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
            boroughSet.cache();

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

            supportClasses3 = Collections.synchronizedList(new ArrayList<>());

            //init
            boroughsNameSet.foreach((ForeachFunction<Row>) row -> {
                  Integer[] localArray = new Integer[weeksNo];
                  for (int j = 0; j < weeksNo; j++) {
                        localArray[j] = 0;
                  }

                  SupportClass3 temp = new SupportClass3();
                  temp.setBorough(row.getString(0));
                  temp.setAccidentsPerBoroughPerWeek(new AtomicReferenceArray<>(localArray));
                  temp.setLethalsPerBoroughPerWeek(0);
                  supportClasses3.add(temp);
            });

            accidentsAndDeathPerBorough.foreach((ForeachFunction<Row>) row -> {
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

                  SupportClass3 toMod = findSupportClass3(row.getString(0));
                  if(toMod != null) {
                        toMod.getAccidentsPerBoroughPerWeek().getAndAccumulate(index, (int) row.getLong(2), Integer::sum);
                        if(row.get(3) != null) {
                              updateLethalAccidents(toMod, (int)row.getLong(3));
                        }
                  }
            });

            System.out.print("\nQUERY 3:\n");
            for (SupportClass3 s : supportClasses3) {
                  System.out.printf("\tBOROUGH: %s\n", s.getBorough());

                  for (int j = 0; j < s.getAccidentsPerBoroughPerWeek().length(); j++) {
                        System.out.printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, s.getAccidentsPerBoroughPerWeek().get(j), "accidents");
                  }

                  System.out.printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                              (100.0f * s.getLethalsPerBoroughPerWeek()) / s.getAccidentsPerBoroughPerWeek().length(),
                              s.getLethalsPerBoroughPerWeek(),
                              s.getAccidentsPerBoroughPerWeek().length());
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

      private static synchronized void updateLethalAccidents(SupportClass3 toMod, int newVal) {
            int prev = toMod.getLethalsPerBoroughPerWeek();
            toMod.setLethalsPerBoroughPerWeek(prev + newVal);
      }


      private static synchronized SupportClass3 findSupportClass3(String name) {
            for(SupportClass3 s : supportClasses3) {
                  if(s.getBorough().equals(name)) {
                        return s;
                  }
            }
            return null;
      }

      private static synchronized SupportClass2 findInListOfSupport(List<SupportClass2> list, String factor) {
            for(SupportClass2 s : list) {
                  if(s.getFactor().equals(factor)) {
                        return s;
                  }
            }
            return null;
      }
}
