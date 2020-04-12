import java.util.concurrent.atomic.AtomicReferenceArray;

public class SupportClass3 {
      private String borough;
      private int [] accidentsPerBoroughPerWeek;
      private Integer lethalsPerBoroughPerWeek;

      public SupportClass3() {
      }

      public SupportClass3(String borough) {
            this.borough = borough;
      }

      public SupportClass3(String borough, int[] accidentsPerBoroughPerWeek, Integer lethalsPerBoroughPerWeek) {
            this.borough = borough;
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
      }

      public String getBorough() {
            return borough;
      }

      public void setBorough(String borough) {
            this.borough = borough;
      }

      public int[] getAccidentsPerBoroughPerWeek() {
            return accidentsPerBoroughPerWeek;
      }

      public void setAccidentsPerBoroughPerWeek(int[] accidentsPerBoroughPerWeek) {
            this.accidentsPerBoroughPerWeek = accidentsPerBoroughPerWeek;
      }

      public Integer getLethalsPerBoroughPerWeek() {
            return lethalsPerBoroughPerWeek;
      }

      public void setLethalsPerBoroughPerWeek(Integer lethalsPerBoroughPerWeek) {
            this.lethalsPerBoroughPerWeek = lethalsPerBoroughPerWeek;
      }
}
