import org.apache.log4j.Level;
import org.apache.log4j.Logger;
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
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.apache.spark.sql.functions.*;

public class NyTest {
      public static void main(String[] args) {
            Logger.getLogger("org").setLevel(Level.OFF);
            Logger.getLogger("akka").setLevel(Level.OFF);


            /* TO RUN IN CLOUD */
            /*
            if(args.length < 1) {
                  System.out.println("ERROR: insert file path and name");
                  return;
            }
            String file = args[0];



            final SparkSession spark = SparkSession
                        .builder()
                        .appName("NyTest")
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



            /*TO RUN IN LOCAL WITHOUT SPECIFYING PARAM*/
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

            /*TO LAUNCH SPECIFYING PARAM*/
            /*
            String file = "";
            String master = "local[4]";
            String core_exec = "";
            String mem_exec = "";
            if(args.length >= 1) {
                  master = args[0];
                  file = args.length >= 2 ? args[1] : "";
                  core_exec = args.length >= 3 ? args[2] : "";
                  mem_exec = args.length >= 4 ? args[3] : "";
            }

            if(file.equals("") || core_exec.equals("") || mem_exec.equals("")) {
                  System.out.println("ERROR: insert file path and name, then num of exec, then core for exec, then mem for exec");
                  return;
            }

            final SparkSession spark = SparkSession
                        .builder()
                        .appName("NyTest")
                        .master(master)
                        .config("spark.executor.cores", core_exec)
                        .config("spark.executor.memory", mem_exec)
                        .getOrCreate();
            */

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
            final Dataset<Row> unorderedDataSet = spark
                        .read()
                        .option("header", "true")
                        .option("dateFormat", "MM/dd/yyyy")
                        .option("delimiter", ",")
                        .schema(schema)
                        .csv(file);

            final Dataset<Row> dataSet = unorderedDataSet
                        .orderBy("date");

            dataSet.persist();

            long loadingTime = System.nanoTime() - startLoading;
            //end of loading


            //query1
            long startQuery1Time = System.nanoTime();

            final Date globalMinDate = dataSet.first().getDate(0);

            final Dataset<Row> descendantDataSet = dataSet
                        .orderBy(col("date").desc())
                        .withColumn("date_diff",
                                    datediff(col("date"), lit(globalMinDate)).plus(1))
                        .select("date",
                                    "borough",
                                    "n_person_killed",
                                    "n_pedestrian_killed",
                                    "n_cyclist_killed",
                                    "n_motor_killed",
                                    "date_diff")
                        ;

            //may be useless
            final Date globalMaxDate = descendantDataSet.first().getDate(0);

            final Dataset<Row> lethalAccidentsSet = dataSet
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
                        .withColumn("lethal_flag",
                                    col("n_person_killed").plus(
                                    col("n_pedestrian_killed").plus(
                                    col("n_cyclist_killed").plus(
                                    col("n_motor_killed")))).gt(0))
                        .filter(col("lethal_flag").equalTo(true))
                        ;
            lethalAccidentsSet.persist();

            int daysNo = descendantDataSet.first().getInt(6);
            int weeksNo = daysNo / 7;
            if((daysNo != 0) && ((daysNo % 7) != 0)) {
                  weeksNo ++;
            }

            AtomicReferenceArray<Integer> lethalAccidents = new AtomicReferenceArray<>(weeksNo);
            //int [] lethalAccidents = new int[weeksNo];
            for(int i = 0; i < weeksNo; i++) {
                  //lethalAccidents[i] = 0;
                  lethalAccidents.set(i, 0);
            }
            int totalLethalAccidents = 0;

            List<Row> lethalAccidentsRow = lethalAccidentsSet.toJavaRDD().collect();
            for(Row r : lethalAccidentsRow) {
                  int index = (int)(ChronoUnit.DAYS.between(globalMinDate.toLocalDate(), r.getDate(0).toLocalDate())) + 1;
                  if(index != 0) {
                        if(index % 7 == 0) {
                              index = (index/7) -1;
                        }
                        else {
                              index = index/7;
                        }
                  }
                  //lethalAccidents[index] ++;
                  lethalAccidents.getAndAccumulate(index, 1, Integer::sum);
                  //lethalAccidents.set(index, );
            }

            System.out.printf("\nQUERY 1:\n");
            for(int i = 0; i < weeksNo; i++) {
                  //System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethalAccidents[i], "lethal accidents");
                  System.out.printf("\tWeek%-5d%-10d%-2s\n", i+1, lethalAccidents.get(i), "lethal accidents");

                  //totalLethalAccidents += lethalAccidents[i];
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

            /*List<String> factors = new ArrayList<>();
            List<Long> accidents = new ArrayList<>();
            List<Long> lethals = new ArrayList<>();*/

            List<SupportClass2> supportClasses2 = Collections.synchronizedList(new ArrayList<>());

            int len = groupedRows.size();
            int rowSize = groupedRows.size() > 0 ? groupedRows.get(0).size() : 0;

            for(int i = 0; i < len; i++) {
                  /*List<String> partialF = new ArrayList<>();
                  List<Long> partialA = new ArrayList<>();
                  List<Long> partialL = new ArrayList<>();*/
                  List<SupportClass2> partialSupport = Collections.synchronizedList(new ArrayList<>());

                  Row row = groupedRows.get(i);
                  for(int j = 0; j < rowSize-2; j++) {
                        //if(row.getString(j) != null && !partialF.contains(row.getString(j))) {
                        if(row.getString(j) != null && findInListOfSupport(partialSupport, row.getString(j)) == null) {
                              /*partialF.add(row.getString(j));
                              partialA.add(row.getLong(rowSize-2));
                              if(row.get(rowSize-1) != null)
                                    partialL.add(row.getLong(rowSize-1));
                              else
                                    partialL.add(0L); //done to respect the size for each list*/
                              SupportClass2 temp = new SupportClass2(
                                          row.getString(j), row.getLong(rowSize-2), 0L);
                              if(row.get(rowSize-1) != null)
                                    temp.setLethals(row.getLong(rowSize-1));
                              partialSupport.add(temp);
                        }
                  }
                  int size = partialSupport.size();
                  for(int k = 0; k < size; k++) {
                        //if(!factors.contains(partialF.get(k))) {
                        SupportClass2 supportFound = findInListOfSupport(supportClasses2, partialSupport.get(k).getFactor());
                        if(supportFound == null) {
                              /*factors.add(partialF.get(k));
                              accidents.add(partialA.get(k));
                              lethals.add(partialL.get(k));*/
                              SupportClass2 temp = new SupportClass2(partialSupport.get(k).getFactor(), partialSupport.get(k).getAccidents(), partialSupport.get(k).getLethals());
                              supportClasses2.add(temp);
                        }
                        else {
                              /*int index = factors.indexOf(partialF.get(k));
                              long prevAcc = accidents.get(index);
                              long prevLeth = lethals.get(index);
                              accidents.set(index, prevAcc + partialA.get(k));
                              lethals.set(index, prevLeth + partialL.get(k));*/
                              supportFound.setAccidents(supportFound.getAccidents() + partialSupport.get(k).getAccidents());
                              supportFound.setLethals(supportFound.getLethals()+ partialSupport.get(k).getLethals());
                        }
                  }
            }

            int facLen = supportClasses2.size();
            System.out.printf("\nQUERY 2:\n");
            System.out.printf("\t%-60s%-20s%-20s%s%n", "FACTOR", "N_ACCIDENTS", "N_DEATHS", "PERC_N_DEATHS");

            double perc = 0f;
            for(int i = 0; i < facLen; i++) {
                  //perc = accidents.get(i) != 0 ? ((lethals.get(i)*100.0f)/accidents.get(i)) : 0;
                  perc = supportClasses2.get(i).getAccidents() != 0 ?
                        ((supportClasses2.get(i).getLethals()*100.0f)/supportClasses2.get(i).getAccidents()) :
                        0;

                  System.out.printf("\t%-60s%-20d%-20d%.2f%%%n", supportClasses2.get(i).getFactor(), supportClasses2.get(i).getAccidents(), supportClasses2.get(i).getLethals(), perc);
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

            /*List<String> boroughs = new ArrayList<>();
            List<Integer> indexes = new ArrayList<>();*/
            List<SupportClass3a> supportClasses3a = Collections.synchronizedList(new ArrayList<>());

            List<Row> boroughDatesData = boroughsNameSet.toJavaRDD().collect();
            for(int i = 0; i < boroughDatesData.size(); i++) {
                  /*boroughs.add(boroughDatesData.get(i).getString(0));
                  if(i == 0) {
                        indexes.add((int) boroughDatesData.get(i).getLong(1));
                  }
                  else {
                        indexes.add((int) boroughDatesData.get(i).getLong(1) + indexes.get(i-1));
                  }*/
                  int indexToAdd = i == 0 ?
                              (int) boroughDatesData.get(i).getLong(1) :
                              (int) boroughDatesData.get(i).getLong(1) + supportClasses3a.get(i-1).getIndex();
                  SupportClass3a temp = new SupportClass3a(boroughDatesData.get(i).getString(0), indexToAdd);
                  supportClasses3a.add(temp);
            }

            /*List<int[]> accidentsPerBoroughPerWeek = new ArrayList<>();
            List<Integer> lethalsPerBoroughPerWeek = new ArrayList<>();
            */
            List<SupportClass3b> supportClasses3b = Collections.synchronizedList(new ArrayList<>());

            //initialization
            for(int i = 0; i < supportClasses3a.size(); i++) {
                  //int[] localArray = new int[weeksNo];
                  Integer[] localArray = new Integer[weeksNo];
                  for(int j = 0; j < weeksNo; j++) {
                        localArray[j] = 0;
                  }

                  /*accidentsPerBoroughPerWeek.add(localArray);
                  lethalsPerBoroughPerWeek.add(0);*/
                  SupportClass3b temp = new SupportClass3b(new AtomicReferenceArray<>(localArray), 0);
                  supportClasses3b.add(temp);
            }


            List<Row> boroughData = accidentsAndDeathPerBorough.toJavaRDD().collect();
            for(int i = 0; i < supportClasses3a.size(); i++) {
                  /*int starting = (i == 0) ? 0 : indexes.get(i-1);
                  for(int j = starting; j < indexes.get(i); j++) {*/
                  int starting = (i == 0) ? 0 : supportClasses3a.get(i-1).getIndex();
                  for(int j = starting; j < supportClasses3a.get(i).getIndex(); j++) {
                        //find the index to update
                        int index = (int)(ChronoUnit.DAYS.between(
                                          globalMinDate.toLocalDate(),
                                          boroughData.get(j).getDate(1).toLocalDate()))
                                    + 1;
                        if(index != 0) {
                              if(index % 7 == 0) {
                                    index = (index/7) -1;
                              }
                              else {
                                    index = index/7;
                              }
                        }

                        /*accidentsPerBoroughPerWeek.get(i)[index] += (int)boroughData.get(j).getLong(2);

                        if(boroughData.get(j).get(3) != null) {
                              int prevLeth = lethalsPerBoroughPerWeek.get(i);
                              lethalsPerBoroughPerWeek.set(i, prevLeth + (int)boroughData.get(j).getLong(3));
                        }*/
                        supportClasses3b.get(i).getAccidentsPerBoroughPerWeek().getAndAccumulate(index, (int)boroughData.get(j).getLong(2), Integer::sum);
                        if(boroughData.get(j).get(3) != null) {
                              int prevLeth = supportClasses3b.get(i).getLethalsPerBoroughPerWeek();
                              supportClasses3b.get(i).setLethalsPerBoroughPerWeek(prevLeth + (int)boroughData.get(j).getLong(3));
                        }
                  }
            }

            System.out.printf("\nQUERY 3:\n");

            for(int i = 0; i < supportClasses3a.size(); i++) {
                  System.out.printf("\tBOROUGH: %s\n", supportClasses3a.get(i).getBorough());

                  for(int j = 0; j < supportClasses3b.get(i).getAccidentsPerBoroughPerWeek().length(); j++) {
                        System.out.printf("\t\tWeek%-5d%-10d%-2s\n", 1 + j, supportClasses3b.get(i).getAccidentsPerBoroughPerWeek().get(j) , "accidents");
                  }

                  System.out.printf("\t\tAvg lethal accidents/week: %.2f%% (%d lethal accidents over %d weeks)\n\n",
                              (100.0f * supportClasses3b.get(i).getLethalsPerBoroughPerWeek())/supportClasses3b.get(i).getAccidentsPerBoroughPerWeek().length(),
                              supportClasses3b.get(i).getLethalsPerBoroughPerWeek(),
                              supportClasses3b.get(i).getAccidentsPerBoroughPerWeek().length());
            }

            long query3Time = System.nanoTime() - startQuery3Time;

            //end of query3

            System.out.println("\nIt took " + loadingTime/1000000000.0f + " seconds to load data");
            System.out.println("It took " + query1Time/1000000000.0f + " seconds to calculate query 1");
            System.out.println("It took " + query2Time/1000000000.0f + " seconds to calculate query 2");
            System.out.println("It took " + query3Time/1000000000.0f + " seconds to calculate query 3");

            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            spark.close();
      }

      private static SupportClass2 findInListOfSupport(List<SupportClass2> list, String factor) {
            for(SupportClass2 s : list) {
                  if(s.getFactor().equals(factor)) {
                        return s;
                  }
            }
            return null;
      }
}
