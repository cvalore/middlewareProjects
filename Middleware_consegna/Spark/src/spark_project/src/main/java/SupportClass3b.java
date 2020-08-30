import java.io.Serializable;

public class SupportClass3b implements Serializable {

      private Integer arraySize;
      private Integer[] accidentsPerBoroughPerWeek;
      private Integer lethalAcc;

      public SupportClass3b(Integer arraySize, Integer lethalAcc) {
            this.arraySize = arraySize;
            this.lethalAcc = lethalAcc;
            this.accidentsPerBoroughPerWeek = new Integer[arraySize];
            for (int i = 0; i < arraySize; i++)
                  accidentsPerBoroughPerWeek[i] = 0;
      }

      public Integer[] getAccidentsPerBoroughPerWeek() {
            return accidentsPerBoroughPerWeek;
      }

      public void addAccident(int index) {
            this.accidentsPerBoroughPerWeek[index]++;
      }

      public Integer getLethalsAcc() {
            return lethalAcc;
      }

      public void addLethalAcc(Integer lethalsPerBoroughPerWeek) {
            this.lethalAcc += lethalsPerBoroughPerWeek;
      }

      public void addLethalAccidents(Integer[] lethalAccidents){
            for (int i = 0; i < arraySize; i++)
                  this.accidentsPerBoroughPerWeek[i] += lethalAccidents[i];
      }

      public Integer getNumAccidents() {
            Integer sum = 0;
            for (int i = 0; i < arraySize; i++)
                  sum += accidentsPerBoroughPerWeek[i];
            return sum;
      }
}
